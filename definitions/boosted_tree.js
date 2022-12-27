
const {create_model} = ml_model;

let model_name = 'contaazul-datalake-lab.ml.model_iris'
let model_from = 'SELECT * FROM `contaazul-datalake-lab.raw.iris`'
let model_type = 'BOOSTED_TREE_CLASSIFIER'
let options = [
        {name: 'BOOSTER_TYPE', value: `'GBTREE'`},
        {name: 'NUM_PARALLEL_TREE', value: 1},
        {name: 'MAX_ITERATIONS', value: 50},
        {name: 'TREE_METHOD', value: `'AUTO'`},
        {name: 'EARLY_STOP', value: 'FALSE'},
        {name: 'SUBSAMPLE', value: 1},
        {name: 'INPUT_LABEL_COLS', value: `['variety']`}
    ]

operate("create_model").queries(create_model(model_name, model_from, model_type, options))