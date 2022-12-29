const topics = [
  "arn:aws:sns:sa-east-1:858038232233:CONTAAZUL_COMPANY_UPDATED",
  "arn:aws:sns:sa-east-1:858038232233:CONTAAZUL_COMPANY_CREATED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_ACQUITTANCE_CREATED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_ACQUITTANCE_DELETED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_ACQUITTANCE_DISCONCERTED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_ACQUITTANCE_REPROCESSED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_ACQUITTANCE_UPDATED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_CHARGE_NOTIFICATION_CREATED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_CHARGE_REMINDER_GROUP_CREATED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_CHARGE_REQUEST_CREATED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_CHARGE_REQUEST_DELETED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_CHARGE_REQUEST_INVALIDATED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_CHARGE_REQUEST_UPDATED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_COST_CENTER_CREATED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_COST_CENTER_CREATION-FAILED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_COST_CENTER_DELETED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_COST_CENTER_UPDATED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_CREATE_COST_CENTER",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_CREATE_FINANCIAL_EVENT",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_FINANCIAL_EVENT_CREATED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_FINANCIAL_EVENT_CREATION-FAILED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_FINANCIAL_EVENT_DELETED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_FINANCIAL_EVENT_UPDATED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_INSTALLMENT_UPDATED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_MONTHLY_BALANCE_RECALCULATE",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_RECONCILE_ACQUITTANCE",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_RECONCILIATION_FAILED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_TRANSFER_CREATED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_TRANSFER_DELETED",
  "arn:aws:sns:sa-east-1:858038232233:FINANCE_PRO_TRANSFER_UPDATED",
  "arn:aws:sns:sa-east-1:858038232233:GOLDENGATE_BANK_FEED_IMPORTED",
  "arn:aws:sns:sa-east-1:858038232233:GOLDENGATE_BANK_STATEMENT_CREATED",
  "arn:aws:sns:sa-east-1:858038232233:GOLDENGATE_CONCILIATION_DELETED",
  "arn:aws:sns:sa-east-1:858038232233:GOLDENGATE_RECONCILIATION_CREATED"
];

let count = 0
let count_tag = 0
let tag = 'transact_topic_0'

topics.forEach(topic => { 
  
  let table = topic.substring(35)
  let query = `WHERE STRING(json_data.TopicArn) = '${topic}' AND
      DATE(publish_time) = current_date()-10`
  if(count % 10 == 0){
    count_tag++
    tag = 'transact_topic_'+count_tag
  }
  count++

  publish(table)
  .type('incremental')
  .tags(tag)
  .schema('bronze_event')
  .bigquery({
    partitionBy: 'DATE(publish_time)',
    requirePartitionFilter : false
  })
  .query(
    ctx => `
      SELECT * FROM ${ctx.ref("transaction_incremental")}
      ${query} AND 
      publish_time > COALESCE((SELECT MAX(publish_time) 
      FROM ${ctx.self()} WHERE DATE(publish_time) >= current_date()-10), '1995-12-31')` 
  ).preOps(ctx => `
    CREATE TABLE IF NOT EXISTS ${ctx.self()} 
    PARTITION BY DATE(publish_time)
    OPTIONS(require_partition_filter=true)
    AS (
      SELECT * FROM ${ctx.ref("transaction_incremental")}
      ${query}
    )
  `);
});














