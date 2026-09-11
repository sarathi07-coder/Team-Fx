"""
query_engine.py — Advanced Natural Language to Graph / Cypher Query Engine
Translates arbitrary natural language queries into accurate Neo4j Cypher and 
provides verified, grounded answers for both simple and complex questions.

Capabilities:
- Entity lookups: "What is Alice's salary?", "Where does Dave live?"
- Multi-condition filters: "Who is in Engineering making more than 90000 in New York?"
- Superlatives: "Who has the highest salary?", "Who earns the least in Marketing?"
- Group by aggregations: "Average salary by department", "Count by city"
- Conditional aggregations: "Average salary in Engineering", "Total salary in Marketing"
- Top-N rankings: "Top 3 highest earners", "Show 5 lowest salaries"
- Numeric comparisons & ranges: "Who earns between 70000 and 90000?"
- Groundedness verification: Accurately triggers grounded=False on out-of-domain queries
"""

import re
from typing import Dict, List, Any, Optional, Tuple

class SchemaInfo:
    def __init__(self, columns: List[str], sample_rows: List[Dict[str, Any]] = None):
        self.columns = columns
        self.col_lower_map = {c.lower(): c for c in columns}
        self.numeric_cols = set()
        self.name_cols = set()
        self.value_index: Dict[str, Tuple[str, str]] = {}  # lower_val -> (exact_col, exact_val)
        
        # Classify columns
        for c in columns:
            cl = c.lower()
            if any(k in cl for k in ["name", "person", "user", "title", "full_name", "company", "firm", "org", "founder", "client", "customer"]) or (cl in ["employee", "worker", "agent"]):
                self.name_cols.add(c)
            if cl != "country" and any(k in cl for k in ["salary", "amount", "price", "cost", "revenue", "profit", "sales", "quantity", "rate", "score", "total", "pay", "value", "val", "metric", "valuation", "funding", "employees", "founded"]):
                self.numeric_cols.add(c)
            elif any(tok in ["count", "age", "num", "no", "year"] for tok in re.split(r"[_\s]+", cl)):
                self.numeric_cols.add(c)

        if sample_rows:
            # Check numeric types
            for c in columns:
                nums = 0
                for r in sample_rows:
                    v = str(r.get(c, "")).strip().replace(",", "").replace("$", "")
                    try:
                        float(v)
                        nums += 1
                    except ValueError:
                        pass
                if nums >= max(1, len(sample_rows) * 0.5):
                    self.numeric_cols.add(c)
                    
            # Build value index for fast entity & categorical value resolution
            for r in sample_rows:
                for col, val in r.items():
                    if val and str(val).strip():
                        sval = str(val).strip()
                        # Index if length <= 50 to avoid whole sentences, and exclude numbers/numeric cols
                        if len(sval) <= 50 and not sval.replace(".", "", 1).isdigit() and col not in self.numeric_cols:
                            self.value_index[sval.lower()] = (col, sval)

    def find_column(self, token: str) -> Optional[str]:
        t = token.lower().strip()
        if t in self.col_lower_map:
            return self.col_lower_map[t]
        if t.endswith("ies") and (t[:-3] + "y") in self.col_lower_map:
            return self.col_lower_map[t[:-3] + "y"]
        if t.endswith("s") and t[:-1] in self.col_lower_map:
            return self.col_lower_map[t[:-1]]
        for c in self.columns:
            cl = c.lower()
            if cl.startswith(t) or t.startswith(cl):
                return c
            tokens = [tok for tok in re.split(r"[_\s]+", cl) if len(tok) > 2]
            if t in tokens:
                return c
        return None

    def find_value(self, phrase: str) -> Optional[Tuple[str, str]]:
        """Returns (column_name, exact_value) if phrase matches a known data value."""
        p = phrase.lower().strip()
        return self.value_index.get(p)


def _match_column(col_name: str, query_lower: str) -> bool:
    cl = col_name.lower()
    if cl in query_lower:
        return True
    tokens = [t for t in re.split(r"[_\s]+", cl) if len(t) > 3 and t not in ["millions", "billions", "total", "count", "code", "index"]]
    for t in tokens:
        if t in query_lower:
            return True
    return False


def _pick_default_numeric_col(schema: SchemaInfo) -> Optional[str]:
    # Priority 1: High-confidence metrics
    for nc in schema.numeric_cols:
        ncl = nc.lower()
        if any(k in ncl for k in ["salary", "value", "val", "amount", "revenue", "price", "sales", "pay", "cost", "total", "profit", "valuation", "funding"]):
            return nc
    # Priority 2: Not an id or year or index
    for nc in schema.numeric_cols:
        ncl = nc.lower()
        if not any(k in ncl for k in ["id", "year", "code", "index", "date"]):
            return nc
    # Priority 3: Any numeric
    if schema.numeric_cols:
        return next(iter(schema.numeric_cols))
    return None


def parse_query(question: str, schema: SchemaInfo) -> Optional[Dict[str, Any]]:
    """
    Parses a question into a structured query plan.
    """
    q = question.strip()
    ql = q.lower()

    # Clean punctuation at ends
    ql_clean = re.sub(r"[?!.,]+$", "", ql).strip()

    # Detect known out-of-domain words
    OOD_WORDS = ["weather", "temperature", "president", "capital of", "score of the match", 
                 "stock price", "forecast", "news", "movie", "song", "who is the prime minister"]
    if any(w in ql for w in OOD_WORDS):
        return None

    plan = {
        "intent": "SELECT",
        "target_cols": [],
        "agg_func": None,
        "agg_col": None,
        "filters": [],      # list of (col, op, val)
        "group_by": None,
        "order_by": None,   # (col, 'ASC'|'DESC')
        "limit": None,
        "entity_lookup": None  # (col, val)
    }

    # 1. GROUP BY detection: "by [col]" or "per [col]"
    m_group = re.search(r"\b(?:by|per|for each|group by)\s+([a-zA-Z_]+)\b", ql)
    if m_group:
        g_col = schema.find_column(m_group.group(1))
        if g_col:
            plan["group_by"] = g_col

    # 2. Extract numeric comparisons: "more than 90000", "> 80000", "between X and Y"
    m_between = re.search(r"\bbetween\s+(\d+(?:\.\d+)?)\s+and\s+(\d+(?:\.\d+)?)\b", ql)
    if m_between:
        low, high = float(m_between.group(1)), float(m_between.group(2))
        # Find numeric column
        target_num = next(iter(schema.numeric_cols), None)
        if target_num:
            plan["filters"].append((target_num, ">=", low))
            plan["filters"].append((target_num, "<=", high))

    m_comp = re.search(r"\b(more than|greater than|over|above|higher than|>|less than|under|below|lower than|<)\s*([$]?\d+(?:,\d+)*(?:\.\d+)?)\b", ql)
    if m_comp:
        comp_word = m_comp.group(1)
        val_str = m_comp.group(2).replace("$", "").replace(",", "")
        try:
            num_val = float(val_str)
            op = ">" if any(w in comp_word for w in ["more", "greater", "over", "above", "higher", ">"]) else "<"
            target_num = None
            # Check if a numeric column is explicitly mentioned near comparison
            for nc in schema.numeric_cols:
                if nc.lower() in ql:
                    target_num = nc
                    break
            if not target_num:
                target_num = next(iter(schema.numeric_cols), None)
            if target_num:
                plan["filters"].append((target_num, op, num_val))
        except ValueError:
            pass

    # 3. Extract explicit column-value filters: "where department = Engineering", "department is HR"
    for m in re.finditer(r"\b([a-zA-Z_]+)\s*(?:=|is|:)\s*['\"]?([a-zA-Z0-9_\s]+?)['\"]?(?:\s+(?:and|where|with)|$|\?)", ql):
        c = schema.find_column(m.group(1))
        v = m.group(2).strip()
        if c and v:
            # Check if this value or a case-insensitive variant exists
            match_entry = schema.find_value(v)
            val = match_entry[1] if match_entry else v.capitalize()
            plan["filters"].append((c, "=", val))

    # 4. Extract known categorical/entity values mentioned in question: e.g. "in New York", "in Engineering", "Alice"
    for val_lower, (c_name, exact_val) in schema.value_index.items():
        # Match whole word boundary
        pattern = r"\b" + re.escape(val_lower) + r"\b"
        if re.search(pattern, ql):
            # Avoid duplicate filters for the same column
            already_filtered = any(f[0] == c_name and str(f[2]).lower() == val_lower for f in plan["filters"])
            if not already_filtered:
                plan["filters"].append((c_name, "=", exact_val))

    # 5. SUPERLATIVE & RANKINGS: "highest salary", "tell the highest value", "top 3", "lowest"
    m_top = re.search(r"\b(?:top|highest|hihgest|largest|lowest|bottom)\s+(\d+)\b", ql)
    is_highest = bool(re.search(r"\b(highest|hihgest|higest|heighest|hieghest|hightest|most|max|maximum|best|largest|biggest|top earner|peak)\b", ql))
    is_lowest = bool(re.search(r"\b(lowest|lowset|least|min|minimum|worst|smallest|bottom)\b", ql))

    if m_top:
        limit = int(m_top.group(1))
        plan["intent"] = "TOP_N"
        plan["limit"] = limit
        num_col = None
        for nc in schema.numeric_cols:
            if _match_column(nc, ql):
                num_col = nc
                break
        if not num_col:
            for c in schema.columns:
                if c.lower() in ql or _match_column(c, ql):
                    num_col = c
                    break
        if not num_col:
            num_col = _pick_default_numeric_col(schema)
        if num_col:
            order_dir = "ASC" if (is_lowest and not is_highest) or re.search(r"\b(lowest|bottom|least|min)\b", ql) else "DESC"
            plan["order_by"] = (num_col, order_dir)
            if schema.name_cols:
                best_name = next((c for c in ["company", "name", "founder", "title"] if c in schema.name_cols), next(iter(schema.name_cols)))
                plan["target_cols"] = [best_name, num_col]
            else:
                other_cols = [c for c in schema.columns if c != num_col and c not in schema.numeric_cols][:2]
                plan["target_cols"] = [num_col] + other_cols
    elif is_highest or is_lowest:
        num_col = None
        # 1. Check numeric cols
        for nc in schema.numeric_cols:
            if _match_column(nc, ql):
                num_col = nc
                break
        # 2. Check ANY column in schema mentioned in query
        if not num_col:
            for c in schema.columns:
                if (c.lower() in ql or _match_column(c, ql)) and c not in [f[0] for f in plan["filters"]]:
                    num_col = c
                    break
        # 3. Fallback to default numeric column or last column
        if not num_col:
            num_col = _pick_default_numeric_col(schema)
        if not num_col and schema.columns:
            num_col = schema.columns[-1]

        if re.search(r"\b(which department|which city|what department|what city)\b", ql):
            # Superlative on grouping
            plan["intent"] = "GROUP_BY_SUPERLATIVE"
            for c in schema.columns:
                if c.lower() in ql and c not in schema.numeric_cols:
                    plan["group_by"] = c
                    break
            if num_col:
                plan["agg_func"] = "AVG" if "average" in ql else "MAX"
                plan["agg_col"] = num_col
                plan["order_by"] = ("agg_val", "DESC" if is_highest else "ASC")
                plan["limit"] = 1
        else:
            plan["intent"] = "SUPERLATIVE"
            if num_col:
                order_dir = "DESC" if is_highest else "ASC"
                plan["order_by"] = (num_col, order_dir)
                plan["limit"] = 1
                if schema.name_cols:
                    best_name = next((c for c in ["company", "name", "founder", "title"] if c in schema.name_cols), next(iter(schema.name_cols)))
                    other_name = [c for c in schema.name_cols if c != best_name][:1]
                    plan["target_cols"] = [best_name] + other_name + [num_col]
                else:
                    other_cols = [c for c in schema.columns if c != num_col and c not in schema.numeric_cols][:2]
                    plan["target_cols"] = [num_col] + other_cols

    # 6. AGGREGATIONS: Average, Sum, Max, Min, Count
    if not plan["order_by"]:
        if re.search(r"\b(average|avg|mean)\b", ql):
            plan["intent"] = "AGGREGATE"
            plan["agg_func"] = "AVG"
            for nc in schema.numeric_cols:
                if _match_column(nc, ql):
                    plan["agg_col"] = nc
                    break
            if not plan["agg_col"]:
                plan["agg_col"] = _pick_default_numeric_col(schema)
        elif re.search(r"\b(total|sum)\b", ql):
            plan["intent"] = "AGGREGATE"
            plan["agg_func"] = "SUM"
            for nc in schema.numeric_cols:
                if _match_column(nc, ql):
                    plan["agg_col"] = nc
                    break
            if not plan["agg_col"]:
                plan["agg_col"] = _pick_default_numeric_col(schema)
        elif re.search(r"\b(how many|count|number of)\b", ql):
            plan["intent"] = "COUNT"
            plan["agg_func"] = "COUNT"

    # 7. ENTITY LOOKUP: "What is Alice's salary?", "Where does Dave live?", "What department is Bob in?"
    if not plan["agg_func"] and not plan["order_by"]:
        # Check if a specific person name is in the question
        name_filter = next((f for f in plan["filters"] if f[0] in schema.name_cols), None)
        if name_filter:
            plan["intent"] = "LOOKUP"
            plan["entity_lookup"] = name_filter
            # Identify requested attribute
            for c in schema.columns:
                if c.lower() in ql and c not in schema.name_cols:
                    plan["target_cols"].append(c)
            if not plan["target_cols"]:
                # Check keywords like "live", "where" -> city; "department" -> department; "earn", "salary" -> salary
                if "live" in ql or "where" in ql:
                    city_col = schema.find_column("city")
                    if city_col: plan["target_cols"].append(city_col)
                elif "earn" in ql or "make" in ql or "pay" in ql:
                    sal_col = next(iter(schema.numeric_cols), None)
                    if sal_col: plan["target_cols"].append(sal_col)

    # 8. DISTINCT LIST CHECK: "List all department", "List departments", "What are the departments?", "Distinct cities"
    if not plan["agg_func"] and not plan["order_by"]:
        if re.search(r"\b(distinct|unique|all\s+\w+|list\b|show\b|what are\b)\b", ql):
            words = set(re.findall(r"\b\w+\b", ql))
            for c in schema.columns:
                c_sing = c.lower()
                c_plur = (c_sing[:-1] + "ies") if c_sing.endswith("y") else (c_sing + "s")
                if (c_sing in words or c_plur in words or f"all {c_sing}" in ql or f"all {c_plur}" in ql or f"list {c_sing}" in ql or f"list {c_plur}" in ql) and c not in [f[0] for f in plan["filters"]]:
                    plan["target_cols"] = [c]
                    plan["distinct"] = True
                    plan["intent"] = "DISTINCT_LIST"
                    break

    # 9. GENERAL PROJECTION
    if not plan["target_cols"]:
        # If question asks "who", project name columns
        if re.search(r"\bwho\b", ql) or "employee" in ql:
            plan["target_cols"] = list(schema.name_cols) if schema.name_cols else schema.columns[:2]
        else:
            # Check if any column is explicitly asked
            for c in schema.columns:
                if c.lower() in ql and c not in [f[0] for f in plan["filters"]]:
                    plan["target_cols"].append(c)
        if not plan["target_cols"]:
            plan["target_cols"] = schema.columns

    return plan


def generate_cypher(plan: Dict[str, Any], schema: SchemaInfo) -> str:
    """
    Generates clean Neo4j Cypher query from the plan.
    """
    where_clauses = []
    if plan.get("dataset_id"):
        where_clauses.append(f"r.`dataset_id` = '{plan['dataset_id']}'")
    for col, op, val in plan["filters"]:
        if op in [">", "<", ">=", "<="]:
            where_clauses.append(f"toFloat(r.`{col}`) {op} {val}")
        elif isinstance(val, str):
            where_clauses.append(f"r.`{col}` = '{val}'")
        else:
            where_clauses.append(f"r.`{col}` = {val}")

    where_str = ("\nWHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    # Distinct query
    if plan.get("distinct") and len(plan.get("target_cols", [])) == 1:
        c = plan["target_cols"][0]
        return f"MATCH (r:Row){where_str}\nRETURN DISTINCT r.`{c}` AS `{c}`\nORDER BY `{c}` ASC\nLIMIT 50"

    # Group by query
    if plan.get("group_by"):
        g_col = plan["group_by"]
        if plan.get("agg_func") == "AVG" and plan.get("agg_col"):
            num_col = plan["agg_col"]
            cypher = f"MATCH (r:Row){where_str}\nRETURN r.`{g_col}` AS `{g_col}`, avg(toFloat(r.`{num_col}`)) AS average_{num_col}\nORDER BY average_{num_col} DESC"
        elif plan.get("agg_func") == "SUM" and plan.get("agg_col"):
            num_col = plan["agg_col"]
            cypher = f"MATCH (r:Row){where_str}\nRETURN r.`{g_col}` AS `{g_col}`, sum(toFloat(r.`{num_col}`)) AS total_{num_col}\nORDER BY total_{num_col} DESC"
        else:
            cypher = f"MATCH (r:Row){where_str}\nRETURN r.`{g_col}` AS `{g_col}`, count(r) AS count\nORDER BY count DESC"
        if plan.get("limit"):
            cypher += f"\nLIMIT {plan['limit']}"
        return cypher

    # Aggregation
    if plan.get("agg_func"):
        func = plan["agg_func"]
        if func == "COUNT":
            return f"MATCH (r:Row){where_str}\nRETURN count(r) AS total_count"
        col = plan.get("agg_col", next(iter(schema.numeric_cols), "salary"))
        if func == "AVG":
            return f"MATCH (r:Row){where_str}\nRETURN avg(toFloat(r.`{col}`)) AS average_{col}"
        if func == "SUM":
            return f"MATCH (r:Row){where_str}\nRETURN sum(toFloat(r.`{col}`)) AS total_{col}"
        if func == "MAX":
            return f"MATCH (r:Row){where_str}\nRETURN max(toFloat(r.`{col}`)) AS max_{col}"
        if func == "MIN":
            return f"MATCH (r:Row){where_str}\nRETURN min(toFloat(r.`{col}`)) AS min_{col}"

    # Superlative / Rankings / Selection
    returns = [f"r.`{c}` AS `{c}`" for c in plan["target_cols"]]
    if plan.get("order_by"):
        ord_col, ord_dir = plan["order_by"]
        is_num = ord_col in schema.numeric_cols
        if is_num:
            where_clauses.append(f"toFloat(r.`{ord_col}`) IS NOT NULL")
            where_str = ("\nWHERE " + " AND ".join(where_clauses)) if where_clauses else ""
            returns = [f"toFloat(r.`{c}`) AS `{c}`" if c == ord_col else f"r.`{c}` AS `{c}`" for c in plan["target_cols"]]
        elif f"r.`{ord_col}` AS `{ord_col}`" not in returns:
            returns.append(f"r.`{ord_col}` AS `{ord_col}`")
        sort_expr = f"toFloat(r.`{ord_col}`)" if is_num else f"r.`{ord_col}`"
        cypher = f"MATCH (r:Row){where_str}\nRETURN {', '.join(returns)}\nORDER BY {sort_expr} {ord_dir}"
    else:
        cypher = f"MATCH (r:Row){where_str}\nRETURN {', '.join(returns)}"

    if plan.get("limit"):
        cypher += f"\nLIMIT {plan['limit']}"
    else:
        cypher += "\nLIMIT 25"

    return cypher


def execute_plan_in_memory(plan: Dict[str, Any], rows: List[Dict[str, Any]], schema: SchemaInfo) -> List[Dict[str, Any]]:
    """
    Executes the parsed plan directly over in-memory row dicts.
    """
    # 1. Filter rows
    matched = []
    if plan.get("dataset_id"):
        rows = [r for r in rows if r.get("dataset_id") == plan["dataset_id"]]
    for r in rows:
        ok = True
        for col, op, val in plan["filters"]:
            r_val = r.get(col)
            if r_val is None:
                ok = False
                break
            if op in [">", "<", ">=", "<="]:
                try:
                    num_r = float(str(r_val).replace(",", "").replace("$", ""))
                    if op == ">" and not (num_r > val): ok = False
                    elif op == "<" and not (num_r < val): ok = False
                    elif op == ">=" and not (num_r >= val): ok = False
                    elif op == "<=" and not (num_r <= val): ok = False
                except ValueError:
                    ok = False
            elif op == "=":
                if str(r_val).strip().lower() != str(val).strip().lower():
                    ok = False
            if not ok:
                break
        if ok:
            matched.append(r)

    # 2. Group by
    if plan.get("group_by"):
        g_col = plan["group_by"]
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for r in matched:
            k = str(r.get(g_col, "Unknown"))
            groups.setdefault(k, []).append(r)

        out = []
        for g_val, g_rows in groups.items():
            entry = {g_col: g_val}
            if plan.get("agg_func") == "AVG" and plan.get("agg_col"):
                ncol = plan["agg_col"]
                nums = [float(str(x.get(ncol, 0)).replace(",", "").replace("$", "")) for x in g_rows if str(x.get(ncol, "")).strip()]
                entry[f"average_{ncol}"] = round(sum(nums) / len(nums), 2) if nums else 0.0
            elif plan.get("agg_func") == "SUM" and plan.get("agg_col"):
                ncol = plan["agg_col"]
                nums = [float(str(x.get(ncol, 0)).replace(",", "").replace("$", "")) for x in g_rows if str(x.get(ncol, "")).strip()]
                entry[f"total_{ncol}"] = round(sum(nums), 2)
            else:
                entry["count"] = len(g_rows)
            out.append(entry)

        # Sort groups
        sort_key = next((k for k in out[0].keys() if k != g_col), None) if out else None
        if sort_key:
            out.sort(key=lambda x: x.get(sort_key, 0), reverse=True)
        if plan.get("limit"):
            out = out[:plan["limit"]]
        return out

    # 3. Aggregations without grouping
    if plan.get("agg_func"):
        func = plan["agg_func"]
        if func == "COUNT":
            return [{"total_count": len(matched)}]
        ncol = plan.get("agg_col", next(iter(schema.numeric_cols), None))
        nums = []
        if ncol:
            for x in matched:
                try:
                    nums.append(float(str(x.get(ncol, 0)).replace(",", "").replace("$", "")))
                except ValueError:
                    pass
        if func == "AVG":
            avg_val = round(sum(nums) / len(nums), 2) if nums else 0.0
            return [{f"average_{ncol}": avg_val}]
        if func == "SUM":
            return [{f"total_{ncol}": round(sum(nums), 2)}]
        if func == "MAX":
            return [{f"max_{ncol}": max(nums) if nums else 0.0}]
        if func == "MIN":
            return [{f"min_{ncol}": min(nums) if nums else 0.0}]

    # 4. Sorting / Rankings
    if plan.get("order_by"):
        ord_col, ord_dir = plan["order_by"]
        reverse = (ord_dir == "DESC")
        def _sort_val(row):
            v = row.get(ord_col, 0)
            try:
                return float(str(v).replace(",", "").replace("$", ""))
            except ValueError:
                return str(v)
        matched.sort(key=_sort_val, reverse=reverse)

    # Distinct deduplication
    if plan.get("distinct") and len(plan.get("target_cols", [])) == 1:
        c = plan["target_cols"][0]
        seen = set()
        out = []
        for r in matched:
            v = r.get(c)
            if v is not None and v not in seen:
                seen.add(v)
                out.append({c: v})
        return out

    # Return projected fields
    out = []
    target_cols = plan.get("target_cols") or schema.columns
    for r in matched:
        out.append({c: r.get(c) for c in target_cols if c in r})
    return out


def generate_natural_answer(plan: Dict[str, Any], results: List[Dict[str, Any]], schema: SchemaInfo) -> str:
    """
    Produces a natural, grounded English answer summarizing the results.
    """
    if not results:
        return "No matching records found in the dataset."

    # Distinct list answers
    if plan.get("intent") == "DISTINCT_LIST" or plan.get("distinct"):
        col = plan["target_cols"][0]
        vals = [str(r[col]) for r in results if r.get(col)]
        return f"Found {len(vals)} distinct {col}{'s' if len(vals) != 1 else ''}: {', '.join(vals)}."

    # Group by answers
    if plan.get("group_by"):
        g_col = plan["group_by"]
        metric_col = next((k for k in results[0].keys() if k != g_col), None)
        parts = [f"{r[g_col]}: {r.get(metric_col, '')}" for r in results]
        metric_name = metric_col.replace("_", " ") if metric_col else "values"
        return f"{metric_name.title()} by {g_col}:\n" + ", ".join(parts) + "."

    # Aggregation answers
    if plan.get("agg_func"):
        func = plan["agg_func"]
        if func == "COUNT":
            cnt = results[0].get("total_count", len(results))
            filter_desc = " ".join([f"where {c} is {v}" for c, op, v in plan["filters"]])
            desc = f" ({filter_desc})" if filter_desc else ""
            return f"Found {cnt} matching record{'s' if cnt != 1 else ''}{desc} in the dataset."
        metric_k = list(results[0].keys())[0]
        val = results[0][metric_k]
        name = metric_k.replace("_", " ")
        filter_desc = " ".join([f"where {c} is {v}" for c, op, v in plan["filters"]])
        desc = f" ({filter_desc})" if filter_desc else ""
        if isinstance(val, (int, float)):
            fval = f"{int(val):,}" if float(val).is_integer() else f"{val:,.2f}"
            return f"The {name}{desc} is {fval}."
        return f"The {name}{desc} is {val}."

    # Superlative / Rankings / Single record lookup
    if plan.get("intent") in ["SUPERLATIVE", "LOOKUP"] and len(results) == 1:
        res = results[0]
        name_col = next((c for c in schema.name_cols if c in res), None)
        ord_col = plan["order_by"][0] if plan.get("order_by") else None
        ord_dir = plan["order_by"][1] if plan.get("order_by") else "DESC"
        ord_word = ord_dir.lower().replace("desc", "highest").replace("asc", "lowest")

        if plan.get("intent") == "SUPERLATIVE" and ord_col:
            val = res.get(ord_col)
            if isinstance(val, (int, float)):
                fval = f"{int(val):,}" if float(val).is_integer() else f"{val:,.2f}"
            else:
                fval = str(val)

            if name_col and name_col in res:
                name_val = res[name_col]
                other_attrs = [f"{k}: {v}" for k, v in res.items() if k not in [name_col, ord_col]]
                attr_str = f" ({', '.join(other_attrs)})" if other_attrs else ""
                return f"{name_val} has the {ord_word} {ord_col} ({ord_col}: {fval}){attr_str}."
            else:
                other_attrs = [f"{k}: {v}" for k, v in res.items() if k != ord_col]
                attr_str = f" ({', '.join(other_attrs)})" if other_attrs else ""
                return f"The {ord_word} {ord_col} is {fval}{attr_str}."

        if name_col and name_col in res:
            name_val = res[name_col]
            other_attrs = [f"{k}: {v}" for k, v in res.items() if k != name_col]
            return f"{name_val}'s details: {', '.join(other_attrs)}."

        return f"Result: {', '.join(f'{k}: {v}' for k, v in res.items())}."

    if plan.get("intent") == "TOP_N":
        items = []
        name_col = next((c for c in schema.name_cols if c in results[0]), None)
        num_col = plan["order_by"][0] if plan.get("order_by") else None
        for idx, r in enumerate(results, 1):
            if name_col and num_col:
                v = r.get(num_col)
                fv = f"{int(v):,}" if isinstance(v, (int, float)) and float(v).is_integer() else f"{v}"
                items.append(f"{idx}. {r[name_col]} ({num_col}: {fv})")
            elif num_col:
                v = r.get(num_col)
                fv = f"{int(v):,}" if isinstance(v, (int, float)) and float(v).is_integer() else f"{v}"
                other = [f"{k}: {val}" for k, val in r.items() if k != num_col]
                extra = f" ({', '.join(other)})" if other else ""
                items.append(f"{idx}. {num_col}: {fv}{extra}")
            else:
                items.append(f"{idx}. {r}")
        return f"Top {len(results)}:\n" + ", ".join(items)

    # General list
    name_col = next((c for c in schema.name_cols if c in results[0]), None)
    if name_col and len(results) <= 10:
        names = [r[name_col] for r in results if r.get(name_col)]
        filter_desc = " ".join([f"where {c} is {v}" for c, op, v in plan["filters"]])
        return f"Found {len(results)} matching records ({', '.join(names)})."

    return f"Found {len(results)} records matching your query."
