function create_query_normalization(json_schema, table_name) {
  
  let query = `WHERE DATE(publish_time) = current_date()-10`

  publish(table_name)
  .type('incremental')
  .tags(table_name)
  .schema('silver_event')
  .bigquery({
    partitionBy: 'DATE(publish_time)'
  })
  .query(
    ctx => `
      SELECT 
        ${json_schema.map((field) => field).join(",\n\t\t")}
      FROM ${ctx.ref('bronze_event', table_name)}
      ${query} AND 
      publish_time > COALESCE((SELECT MAX(TIMESTAMP(STRING(publish_time))) 
      FROM ${ctx.self()} WHERE DATE(STRING(publish_time)) >= current_date()-10), '1995-12-31')` 
  ).preOps(ctx => `
    CREATE TABLE IF NOT EXISTS ${ctx.self()} 
    PARTITION BY DATE(publish_time)
    AS (
      SELECT 
        ${json_schema.map((field) => field).join(",\n\t\t")}
      FROM ${ctx.ref('bronze_event', table_name)}
      ${query}
    )
  `);
}

module.exports = { create_query_normalization };