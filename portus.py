import sys
import inspect
import re
import logging
from typing import Optional

from sqlalchemy import inspect as sqla_inspect

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def init():
    """
    Monkey-patch an already-imported pandas module in the current execution context
    by adding a `read_ai` function. This function will:
      - Fetch SQL schema information from the provided engine
      - Use a LangChain LLM to generate an SQL query based on the prompt and schema
      - Execute the generated query and return a DataFrame
    The function does NOT import pandas and does NOT return the module.
    It only patches if pandas is already imported in the same context.
    """
    pd_mod = sys.modules.get("pandas")

    # If not found via sys.modules, try to detect a pandas module object in caller globals.
    if pd_mod is None:
        # noinspection PyUnusedLocal
        caller_frame = None
        try:
            caller_frame = inspect.currentframe().f_back
            if caller_frame is not None:
                for val in caller_frame.f_globals.values():
                    if getattr(val, "__name__", None) == "pandas":
                        pd_mod = val
                        break
        finally:
            # Help GC by removing frame references
            del caller_frame

    if pd_mod is None:
        # pandas is not imported in the current context; nothing to patch
        return

    if hasattr(pd_mod, "read_ai"):
        # Already patched; do nothing
        return

    def _gather_schema(engine, target_schema: Optional[str] = None, table_limit: int = 100, cols_limit: int = 50):
        inspector = sqla_inspect(engine)
        dialect_name = getattr(engine.dialect, "name", "unknown")
        default_schema = getattr(engine.dialect, "default_schema_name", None)

        # Choose a schema to introspect
        schema_name = target_schema or default_schema

        # Fallback: try to find a likely user schema if default not available
        if not schema_name:
            # noinspection PyBroadException
            try:
                schemas = inspector.get_schema_names()
            except Exception:
                schemas = []
            # Prefer public for Postgres, dbo for SQL Server, else first non-system
            preferred = ["public", "dbo"]
            schema_name = next((s for s in preferred if s in schemas), None) or next(
                (s for s in schemas if not s.startswith("pg_") and not s.startswith("information_schema")), None
            )

        # Collect tables
        # noinspection PyBroadException
        try:
            table_names = inspector.get_table_names(schema=schema_name)[:table_limit]
        except Exception:
            table_names = []

        # Build schema text
        lines = [f"-- Dialect: {dialect_name}"]
        if schema_name:
            lines.append(f"-- Schema: {schema_name}")
        for t in table_names:
            # noinspection PyBroadException
            try:
                cols = inspector.get_columns(t, schema=schema_name)[:cols_limit]
            except Exception:
                cols = []
            col_parts = []
            for c in cols:
                cname = c.get("name", "unknown")
                ctype = str(c.get("type", ""))
                nullable = c.get("nullable", True)
                col_parts.append(f"{cname} {ctype}{' NULL' if nullable else ' NOT NULL'}")
            if col_parts:
                lines.append(f"TABLE {t} (")
                for i, part in enumerate(col_parts):
                    comma = "," if i < len(col_parts) - 1 else ""
                    lines.append(f"  {part}{comma}")
                lines.append(")")
            else:
                lines.append(f"TABLE {t}")
        return "\n".join(lines).strip(), (schema_name or None), getattr(engine.dialect, "name", "unknown")

    def _ensure_llm(llm: Optional[object] = None, model: Optional[str] = None, temperature: float = 0.0):
        """
        Try a few import paths for a Chat LLM. Prefer a provided llm instance.
        """
        if llm is not None:
            return llm

        try:
            from langchain_openai import ChatOpenAI  # type: ignore

            return ChatOpenAI(model=model or "gpt-4o-mini", temperature=temperature)
        except Exception as e:
            raise ImportError(
                "LangChain LLM is required but not installed/available. "
                "Install and configure a LangChain-compatible chat model, e.g.:\n"
                "  - pip/conda install langchain-openai (and set OPENAI_API_KEY), or\n"
                "  - provide an instantiated `llm` via read_ai(..., llm=your_llm)\n"
                f"Import errors: {e!r}"
            )

    def _build_system_prompt(dialect: str, limit_rows: Optional[int]) -> str:
        limit_clause = f"Include 'LIMIT {limit_rows}' (or the dialect equivalent) unless a LIMIT/OFFSET is already present." if limit_rows else ""
        return (
            "You are a senior data analyst that writes correct, efficient SQL for the given database schema.\n"
            f"- Use SQL dialect: {dialect}.\n"
            "- Only output a single executable SQL query without explanations or markdown fences.\n"
            "- Prefer selecting explicit columns and include necessary JOIN conditions.\n"
            f"- {limit_clause}\n"
            "- If something is ambiguous, make reasonable assumptions."
        ).strip()

    def _strip_sql_fences(text: str) -> str:
        # Remove ```sql ... ``` or ``` ... ``` fences
        text = text.strip()
        fence_block = re.compile(r"^```(?:sql)?\s*([\s\S]*?)\s*```$", re.IGNORECASE)
        m = fence_block.match(text)
        if m:
            return m.group(1).strip()
        # Remove inline triple backticks if present
        text = re.sub(r"```", "", text).strip()
        # Quick fix for sqlalchemy params
        text = text.replace("%", "")
        # Also remove surrounding backticks if present
        return text.strip("` \n\r\t")

    def read_ai(
            prompt,
            engine=None,
            *,
            target_schema: Optional[str] = None,
            table_limit: int = 100,
            columns_per_table_limit: int = 50,
            model: Optional[str] = None,
            temperature: float = 0.0,
            llm: Optional[object] = None,
            max_rows: Optional[int] = 1000,
            **kwargs,
    ):
        """
        Parameters:
        - prompt: str. Natural language instruction or question.
        - engine: SQLAlchemy engine/connection (required).
        - target_schema: optional schema name to introspect (defaults to engine default).
        - table_limit: limit number of tables included in the schema prompt.
        - columns_per_table_limit: limit number of columns per table in the schema prompt.
        - model: optional model name for the LLM (depends on provider).
        - temperature: LLM temperature.
        - llm: optional pre-instantiated LangChain chat model. If not provided, a default will be created.
        - max_rows: desired LIMIT to suggest to the LLM if query doesn't include a limit.
        - **kwargs: forwarded to pandas.read_sql for execution (e.g., params=...).

        Returns: pandas.DataFrame with the result of executing the generated SQL.
        """
        if engine is None:
            raise ValueError("read_ai requires an SQLAlchemy engine/connection (engine=...).")

        # 1) Fetch schema from engine
        schema_text, resolved_schema, dialect_name = _gather_schema(
            engine, target_schema=target_schema, table_limit=table_limit, cols_limit=columns_per_table_limit
        )
        logger.info("Using dialect=%s, schema=%s", dialect_name, resolved_schema)
        logger.debug("Fetched DB schema overview:\n%s", schema_text)

        # 2) Build prompts and call LLM (LangChain)
        chat_llm = _ensure_llm(llm=llm, model=model, temperature=temperature)
        try:
            # Support both message-based and string-based invocation
            system_prompt = _build_system_prompt(dialect_name, max_rows)
            user_prompt = (
                f"Prompt:\n{prompt}\n\n"
                f"Database schema overview:\n{schema_text}\n\n"
                "Return only the SQL query:"
            )
            # Message style if supported (most modern ChatModels support dict-like messages)
            try:
                result = chat_llm.invoke(
                    [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ]
                )
                content = getattr(result, "content", result)  # Some models return object with .content
                sql_query = _strip_sql_fences(content if isinstance(content, str) else str(content))
            except TypeError:
                # Fallback to simple string invocation
                combined = system_prompt + "\n\n" + user_prompt
                result = chat_llm.invoke(combined)  # type: ignore
                content = getattr(result, "content", result)
                sql_query = _strip_sql_fences(content if isinstance(content, str) else str(content))
        except Exception as e:
            raise RuntimeError(f"LLM generation failed: {e}") from e

        if not isinstance(sql_query, str) or not sql_query.strip():
            raise RuntimeError("LLM did not return a valid SQL string.")

        logger.info("%s", sql_query)

        # 3) Execute the generated SQL and return DataFrame
        return pd_mod.read_sql(sql_query, engine, **kwargs)

    setattr(pd_mod, "read_ai", read_ai)
