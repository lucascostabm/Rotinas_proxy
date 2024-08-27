WITH atendimentos AS (
SELECT DISTINCT
	atendi.chamada_numero,
	atendi.chamada_data_hora_inclusao
FROM db_bisp_cad_reporting.tb_atendimento atendi
WHERE 1=1
	AND atendi.grupo_atendimento_id = 4 --Chamadas somente do COBOM-RMBH
	AND atendi.chamada_numero IS NOT NULL
	AND YEAR(atendi.chamada_data_hora_inclusao) =:ANO_FATO
),

reds_integracao AS (
SELECT
	reds.chamada_atendimento_id,
	reds.chamada_numero,
	reds.reds_numero,
    ROW_NUMBER() OVER (
        PARTITION BY reds.chamada_numero
        ORDER BY reds.chamada_numero
    ) AS rn
FROM db_bisp_cad_reporting.tb_integracao_reds reds
WHERE 1=1
	AND reds.unidade_reds_orgao_id = 2 -- Integrações feita pelo CBMMG
),

empenhos AS (
SELECT
	emp.chamada_atendimento_id,
	GROUP_CONCAT(emp.recurso_codigo_prefixo, ', ') AS recurso_codigo_prefixo
FROM db_bisp_cad_reporting.tb_empenho emp
WHERE 1=1
	AND emp.unidade_servico_codigo_tipo = 'BM'
	AND YEAR(emp.empenho_data_hora_inicio) = :ANO_FATO
GROUP BY 1
),

chamadas AS (
SELECT DISTINCT
	cham.chamada_numero, 
	FROM_TIMESTAMP(cham.chamada_data_hora_inclusao, 'dd/MM/yyyy') as 'chamada_data_inclusao',
	FROM_TIMESTAMP(cham.chamada_data_hora_inclusao, 'HH:mm:ss') AS 'chamada_hora_inclusao',
	cham.chamada_data_hora_inclusao, 
	cham.chamada_atendimento_id,
	cham.chamada_classificacao_descricao,
	FROM_TIMESTAMP(cham.chamada_classificacao_data_hora, 'dd/MM/yyyy') as 'chamada_classificacao_data',
	FROM_TIMESTAMP(cham.chamada_classificacao_data_hora, 'HH:mm:ss') AS 'chamada_classificacao_hora',
	cham.chamada_classificacao_data_hora,
    CASE 
        WHEN cham.chamada_classificacao_data_hora IS NULL OR cham.chamada_classificacao_data_hora = '' THEN ''
        ELSE 'CLASSIFICADA'
    END AS ESTADO_CHAMADA,
	cham.local_latitude,
	cham.local_longitude,
	cham.local_municipio_id,
	cham.local_municipio_nome,
	CONCAT(cham.local_tipo_logradouro_nome,  '-', cham.local_logradouro_nome, '-', cham.local_bairro_nome) AS 'LOCAL_DO_FATO',
	cham.natureza_codigo,
	cham.natureza_descricao,
	cham.unidade_servico_codigo,
	cham.unidade_servico_nome
FROM db_bisp_cad_reporting.tb_chamada_atendimento cham
WHERE 1=1
	AND cham.orgao_id = 2
	AND YEAR(cham.chamada_data_hora_inclusao) = :ANO_FATO
)

SELECT 
	atendimentos.chamada_numero,
	chamadas.chamada_classificacao_descricao,
	chamadas.local_latitude,
	chamadas.local_longitude,
	chamadas.local_municipio_id,
	chamadas.local_municipio_nome,	
	chamadas.LOCAL_DO_FATO,
	chamadas.natureza_codigo,
	chamadas.natureza_descricao,	
	chamadas.unidade_servico_codigo,
	chamadas.unidade_servico_nome,
	reds_integracao.reds_numero,
	empenhos.recurso_codigo_prefixo,
	chamadas.chamada_data_inclusao,
	chamadas.chamada_hora_inclusao,
	chamadas.chamada_classificacao_data,
	chamadas.chamada_classificacao_hora,
	chamadas.ESTADO_CHAMADA
FROM atendimentos
LEFT JOIN reds_integracao ON atendimentos.chamada_numero = reds_integracao.chamada_numero AND reds_integracao.rn = 1
LEFT JOIN empenhos ON empenhos.chamada_atendimento_id = reds_integracao.chamada_atendimento_id
JOIN chamadas 
WHERE 1=1 
	AND atendimentos.chamada_numero = chamadas.chamada_numero