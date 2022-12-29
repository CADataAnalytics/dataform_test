const json_schema = [
    'json_data.Message.tenantId',
	'json_data.Message.message.active',
	'json_data.Message.message.bankId',
	'STRING(json_data.Message.message.description) as description',
	'STRING(json_data.Message.message.source) as source',
	'json_data.Message.message.id',
	'json_data.Message.message.versionId',
	'STRING(json_data.Message.message.suggestionType) as suggestionType',
	'json_data.Message.user',
	'json_data.Message.message.value as value',
	'TIMESTAMP(STRING(json_data.Message.message.date)) as date',
	'STRING(json_data.Message.createdAt) as createdAt',
	'TIMESTAMP(STRING(publish_time)) as publish_time'
]

let table_name = 'GOLDENGATE_BANK_STATEMENT_CREATED'

json_normalization.create_query_normalization(json_schema, table_name)




