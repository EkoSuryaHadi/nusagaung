/**
 * Dashboard Filter Utility for dynamic SQL query generation in DuckDB.
 * Supports Date Range, Category, Search, and Status filters.
 */

export interface DashboardFilter {
  field: string;
  operator: "EQUALS" | "NOT_EQUALS" | "GREATER_THAN" | "LESS_THAN" | "BETWEEN" | "IN" | "CONTAINS";
  value: any;
}

export function buildFilterWhereClause(filters: DashboardFilter[]): string {
  if (!filters || filters.length === 0) return "";

  const clauses: string[] = [];

  for (const filter of filters) {
    if (!filter.field || filter.value === undefined || filter.value === null || filter.value === "") {
      continue;
    }

    const fieldSanitized = filter.field.replace(/[^a-zA-Z0-9_.]/g, "");

    switch (filter.operator) {
      case "EQUALS":
        clauses.push(`"${fieldSanitized}" = '${String(filter.value).replace(/'/g, "''")}'`);
        break;

      case "NOT_EQUALS":
        clauses.push(`"${fieldSanitized}" != '${String(filter.value).replace(/'/g, "''")}'`);
        break;

      case "GREATER_THAN":
        clauses.push(`"${fieldSanitized}" >= '${String(filter.value).replace(/'/g, "''")}'`);
        break;

      case "LESS_THAN":
        clauses.push(`"${fieldSanitized}" <= '${String(filter.value).replace(/'/g, "''")}'`);
        break;

      case "BETWEEN":
        if (Array.isArray(filter.value) && filter.value.length === 2) {
          const val1 = String(filter.value[0]).replace(/'/g, "''");
          const val2 = String(filter.value[1]).replace(/'/g, "''");
          clauses.push(`"${fieldSanitized}" BETWEEN '${val1}' AND '${val2}'`);
        }
        break;

      case "IN":
        if (Array.isArray(filter.value) && filter.value.length > 0) {
          const items = filter.value.map((v) => `'${String(v).replace(/'/g, "''")}'`).join(", ");
          clauses.push(`"${fieldSanitized}" IN (${items})`);
        }
        break;

      case "CONTAINS":
        clauses.push(`"${fieldSanitized}" ILIKE '%${String(filter.value).replace(/'/g, "''")}%'`);
        break;
    }
  }

  if (clauses.length === 0) return "";
  return `WHERE ${clauses.join(" AND ")}`;
}
