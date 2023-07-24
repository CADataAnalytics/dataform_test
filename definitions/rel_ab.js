const {crie_relacionamento} = join_sql;

let nome_processo = 'ab_apps'
let tabela1 = 'sandbox.public_abs'
let campos_tb1 = ['percentile', 'active']
let chave1 = 'app_id'
let tabela2 = 'sandbox.public_apps'
let campos_tb2 = ['name']
let chave2 = 'id'
let quando = ['tb1.id > 1', 'tb1.active = true']

publish(nome_processo)
.type('table')
.query(
    crie_relacionamento(
        campos_tb1, 
        tabela1, 
        chave1, 
        campos_tb2, 
        tabela2, 
        chave2, 
        quando
    )
)