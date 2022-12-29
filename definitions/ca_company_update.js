const json_schema = [
    'json_data.Message.tenantId',
	'json_data.Message.message.active',
	'json_data.Message.message.address',
	'STRING(json_data.Message.message.email) as email',
	'STRING(json_data.Message.message.fantasyName) as fantasyName',
	'json_data.Message.message.isCompanyDemo',
	'json_data.Message.message.isentaInscricaoEstadual',
	'STRING(json_data.Message.message.name) as name',
	'STRING(json_data.Message.message.phone) as phone',
	'json_data.Message.user',
	'CAST(STRING(json_data.Message.version) as FLOAT64) as version',
	'TIMESTAMP(STRING(json_data.Message.createdAt)) as createdAt',
	'TIMESTAMP(STRING(publish_time)) as publish_time'
]

let table_name = 'CONTAAZUL_COMPANY_CREATED'

json_normalization.create_query_normalization(json_schema, table_name)