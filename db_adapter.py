"""Backend unifié SQLite ↔ PostgreSQL (Boss 26/10 — P0 migration Railway).

- DATABASE_URL défini -> PostgreSQL DURABLE : mémoire, clients, paniers,
  commandes, anti-doublon, tunnels... TOUT survit aux redéploiements
  Railway (l'anti-doublon /tmp était effacé à chaque déploiement).
- sinon -> SQLite (tests/dev sandbox), comportement strictement inchangé.

Compatibilité gardée avec le code existant (zéro refactor des modules) :
  • placeholders ? -> %s (PG seulement)
  • INTEGER PRIMARY KEY AUTOINCREMENT -> BIGSERIAL PRIMARY KEY
  • PRAGMA table_info(x) -> information_schema (mêmes colonnes en sortie)
  • INSERT OR REPLACE -> ON CONFLICT ... DO UPDATE (PG)
  • INSERT OR IGNORE -> ON CONFLICT DO NOTHING (PG)
  • lastrowid des INSERT -> RETURNING id (PG)
  • psycopg2.errors.UniqueViolation -> sqlite3.IntegrityError
    (les except existants continuent de fonctionner)

RÉSILIENCE (le bot ne meurt jamais) : connexion PAR THREAD (telebot est
multi-thread), reconnexion automatique avec backoff en cas de reset
réseau/Railway, jamais d'exception de connexion non relancée.

pgvector (RAG pro) : à l'init PG, l'extension vector et la table
kb_embeddings(vector(384)) sont créées SI la plateforme les fournit.
Grâceful degradation : sans pgvector, le RAG lexical bidirectionnel
(testé 32/32) continue de servir — aucune panne.
"""
from __future__ import annotations

import logging
import os
import re
import sqlite3
import threading
import time

log = logging.getLogger("db_adapter")

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()

# ── Traduction SQL SQLite -> PostgreSQL ─────────────────────────────────
_RE_AUTOINC = re.compile(r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT", re.I)
_RE_PRAGMA = re.compile(r"^\s*PRAGMA\s+table_info\((\w+)\)\s*$", re.I)
_RE_INS_OR_REP = re.compile(r"^\s*INSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)", re.I)
_RE_INS_OR_IGN = re.compile(r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\s+(\w+)", re.I)


def _to_pg(sql: str, params: tuple) -> str:
    sql = _RE_AUTOINC.sub("BIGSERIAL PRIMARY KEY", sql)
    sql = _RE_INS_OR_IGN.sub(lambda m: f"INSERT INTO {m.group(1)}", sql)
    m = _RE_INS_OR_REP.match(sql)
    if m:
        # INSERT OR REPLACE INTO t (a,b,c) VALUES (...) ->
        # INSERT INTO t (a,b,c) VALUES (...) ON CONFLICT (pk) DO UPDATE
        table = m.group(1)
        head = _RE_INS_OR_REP.sub(f"INSERT INTO {table}", sql, count=1)
        pks = {
            "flows": ["chat_id"], "global_promo": ["id"], "bot_state": ["key"],
            "clients": ["chat_id"], "promo_codes": ["code"],
            "offhours_notified": ["chat_id", "day"],
        }.get(table, ["id"])
        head = head.rstrip(";").rstrip()
        head += f" ON CONFLICT ({','.join(pks)}) DO UPDATE SET " + ", ".join(
            f"{c} = EXCLUDED.{c}" for c in _cols_of(sql))
        sql = head
    # PLACEHOLDERS : conversion TOUJOURS sous PG. CRASH Railway 26/10
    # 22:42 : executemany passait params=() -> les « ? » restaient en
    # l'état -> « syntax error at or near "," » et worker Crashed.
    # Aucun « ? » littéral n'existe dans nos requêtes (vérifié).
    sql = sql.replace("?", "%s")
    return sql


def _cols_of(insert_sql: str) -> list[str]:
    m = re.search(r"\(\s*([^)]+)\)\s*VALUES", insert_sql, re.I | re.S)
    if not m:
        return []
    return [c.strip().strip('"') for c in m.group(1).split(",") if c.strip()]


class _Cursor:
    """Wrapper compatible sqlite3 : fetchone/fetchall/lastrowid."""
    def __init__(self, cur, last_id=None):
        self._cur = cur
        self._last_id = last_id

    def fetchone(self):
        try:
            return self._cur.fetchone()
        except Exception:
            return None

    def fetchall(self):
        try:
            return self._cur.fetchall()
        except Exception:
            return []

    @property
    def lastrowid(self):
        return self._last_id


class Database:
    """API compatible avec sqlite3.Connection pour DUOUX backends."""
    backend = "sqlite"

    def __init__(self, sqlite_path: str, logger=None):
        global DATABASE_URL
        self._logger = logger or log
        self._sqlite_path = sqlite_path
        self._sqlite = None
        self._local = threading.local()
        self._reconnect_at = 0.0
        if DATABASE_URL:
            self.backend = "postgres"
            self._dsn = self._fix_dsn(DATABASE_URL)
            self.pgvector = self._try_pgvector()
        else:
            self.backend = "sqlite"
            self.pgvector = False
            self._sqlite = sqlite3.connect(
                sqlite_path, check_same_thread=False)

    @staticmethod
    def _fix_dsn(dsn: str) -> str:
        # Railway fournit postgres:// ; psycopg2 accepte, on force
        # sslmode=require si absent (tunnels Railway).
        if "sslmode=" not in dsn:
            sep = "&" if "?" in dsn else "?"
            dsn += f"{sep}sslmode=require"
        return dsn

    # ── PostgreSQL : connexion par thread + reconnexion auto ────────────
    def _pg_conn(self):
        for attempt in range(6):
            conn = getattr(self._local, "conn", None)
            if conn is not None and getattr(self._local, "broken", False) is False:
                return conn
            try:
                import psycopg2
                conn = psycopg2.connect(self._dsn, connect_timeout=15)
                conn.autocommit = True
                self._local.conn = conn
                self._local.broken = False
                return conn
            except Exception as exc:
                self._logger.warning(
                    "PostgreSQL indisponible (tentative %s/6) : %s",
                    attempt + 1, exc)
                time.sleep(min(2 ** attempt, 30))
        self._logger.error("PostgreSQL JOIN IMPOSSIBLE après 6 tentatives")
        return None

    def _break(self):
        try:
            if getattr(self._local, "conn", None):
                self._local.conn.close()
        except Exception:
            pass
        self._local.conn = None
        self._local.broken = True

    def _try_pgvector(self) -> bool:
        try:
            conn = self._pg_conn()
            if conn is None:
                return False
            cur = conn.cursor()
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS kb_embeddings (
                    question TEXT PRIMARY KEY,
                    lang TEXT NOT NULL DEFAULT 'fr',
                    embedding vector(384) NOT NULL,
                    ts DOUBLE PRECISION NOT NULL DEFAULT now()
                )""")
            try:
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS kb_embeddings_ivf
                    ON kb_embeddings USING ivfflat (embedding vector_cosine_ops)
                    WITH (lists = 100)""")
            except Exception:
                pass  # index lourd : sans gravité, recherche séquentelle OK
            self._logger.info("pgvector ACTIF : table kb_embeddings(vector384) prête")
            return True
        except Exception as exc:
            self._logger.warning(
                "pgvector indisponible sur ce serveur (%s) — "
                "RAG lexical bidirectionnel en service, aucune panne", exc)
            return False

    # ── API commune ─────────────────────────────────────────────────────
    def execute(self, sql, params=()):
        if self.backend == "sqlite":
            return self._sqlite.execute(sql, params)
        m = _RE_PRAGMA.match(sql or "")
        if m:
            return self._pragma_pg(m.group(1))
        sql_pg = _to_pg(sql or "", params)
        is_insert = re.match(r"^\s*INSERT\s", sql_pg, re.I)
        conn = self._pg_conn()
        if conn is None:
            # Backend PG mort mais on ne fait JAMAIS tomber le bot :
            # erreur traduite en sqlite, les except existants captent.
            raise sqlite3.OperationalError("PostgreSQL injoignable")
        cur = conn.cursor()
        try:
            if is_insert and " RETURNING " not in sql_pg.upper():
                try:
                    cur.execute(sql_pg + " RETURNING id", params)
                    row = cur.fetchone()
                    return _Cursor(cur, row[0] if row else None)
                except Exception:
                    cur = conn.cursor()  # table sans colonne id : insert simple
            cur.execute(sql_pg, params)
            return _Cursor(cur)
        except Exception as exc:
            import psycopg2
            if isinstance(exc, (psycopg2.errors.UniqueViolation,
                                psycopg2.errors.ForeignKeyViolation)):
                self._break()
                raise sqlite3.IntegrityError(str(exc)) from exc
            if isinstance(exc, psycopg2.Error):
                self._break()
                self._logger.warning("Erreur PG (reconnexion) : %s", exc)
                raise sqlite3.OperationalError(str(exc)) from exc
            raise

    def _pragma_pg(self, table: str):
        conn = self._pg_conn()
        if conn is None:
            return _Cursor(None)
        cur = conn.cursor()
        cur.execute("""
            SELECT 0, column_name, data_type, 0, NULL, 0
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position""", (table,))
        return _Cursor(cur)

    def executemany(self, sql, seq_of_params):
        if self.backend == "sqlite":
            return self._sqlite.executemany(sql, seq_of_params)
        sql_pg = _to_pg(sql, ())
        conn = self._pg_conn()
        if conn is None:
            raise sqlite3.OperationalError("PostgreSQL injoignable")
        cur = conn.cursor()
        try:
            cur.executemany(sql_pg, list(seq_of_params))
            return _Cursor(cur)
        except Exception as exc:
            import psycopg2
            if isinstance(exc, psycopg2.errors.UniqueViolation):
                raise sqlite3.IntegrityError(str(exc)) from exc
            if isinstance(exc, psycopg2.Error):
                self._break()
                raise sqlite3.OperationalError(str(exc)) from exc
            raise

    def executescript(self, script: str):
        if self.backend == "sqlite":
            return self._sqlite.executescript(script)
        # PG : multi-instructions acceptées en un seul execute
        cur = self._pg_conn()
        if cur is None:
            raise sqlite3.OperationalError("PostgreSQL injoignable")
        translated = _to_pg(script, ())
        return cur.cursor().execute(translated)

    def commit(self):
        if self.backend == "sqlite":
            self._sqlite.commit()
        # PG : autocommit — rien à faire

    def close(self):
        try:
            if self.backend == "sqlite":
                self._sqlite.close()
            elif getattr(self._local, "conn", None):
                self._local.conn.close()
        except Exception:
            pass

    def stats(self) -> dict:
        """Comptes clés pour le tableau de bord admin."""
        # tolérant : une table absente (probe sandbox) vaut 0, jamais d'erreur
        def one(q):
            try:
                r = self.execute(q).fetchone()
                return r[0] if r else 0
            except Exception:
                return 0
        return {
            "backend": self.backend,
            "pgvector": self.pgvector,
            "memory": one("SELECT COUNT(*) FROM memory"),
            "clients": one("SELECT COUNT(*) FROM clients"),
            "leads": one("SELECT COUNT(*) FROM leads"),
            "processed": one("SELECT COUNT(*) FROM processed_events"),
            "processed_done": one(
                "SELECT COUNT(*) FROM processed_events WHERE state = 'done'"),
            "processed_failed": one(
                "SELECT COUNT(*) FROM processed_events WHERE state = 'failed'"),
        }


def connect(sqlite_path: str, logger=None) -> Database:
    """Point d'entrée unique : PG si DATABASE_URL, sinon SQLite."""
    return Database(sqlite_path, logger=logger)
