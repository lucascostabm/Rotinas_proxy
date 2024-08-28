WITH OCORRENCIA AS (
SELECT 
	oco.numero_ocorrencia,
	oco.numero_chamada_cad,
	oco.id_unid_resp_registro,
	oco.numero_latitude AS 'latitude_reds',
	oco.numero_longitude AS 'longitude_reds',
	oco.natureza_codigo, 
	oco.codigo_municipio 
FROM db_bisp_reds_reporting.tb_ocorrencia oco
WHERE 1=1
	AND (SUBSTRING(oco.natureza_codigo, 1, 3) IN ('O01', 'O02', 'O03', 'R03', 'S01', 'S03', 'S04', 'S05', 'S07', 'V01', 'V02', 'V03', 'V04', 'R04')
	OR oco.natureza_codigo IN ('S04013', 'S04015', 'S04016', 'S04017', 'S04018', 'S04019', 'S04099'))
	AND ind_estado IN ('F', 'R')
	AND YEAR(oco.data_hora_fato) = :ANO_FATO
),

CHAMADA AS (
    WITH chamada_com_ordenacao AS (
        SELECT 
            cham.chamada_numero,
            cham.chamada_atendimento_id,
            cham.local_latitude AS latitude_cad,
            cham.local_longitude AS longitude_cad,
            FROM_TIMESTAMP(cham.chamada_data_hora_inclusao, 'dd/MM/yyyy') AS DATA_HC,
            FROM_TIMESTAMP(cham.chamada_data_hora_inclusao, 'HH:mm:ss') AS HORA_HC,
            ROW_NUMBER() OVER (PARTITION BY cham.chamada_numero, cham.chamada_atendimento_id ORDER BY cham.chamada_data_hora_inclusao) AS rn
        FROM db_bisp_cad_reporting.tb_chamada_atendimento cham
        WHERE 1=1
            AND cham.orgao_id = 2
            AND YEAR(cham.chamada_data_hora_inclusao) = :ANO_FATO
            AND cham.chamada_classificacao_data_hora IS NOT NULL
    )
    SELECT
        chamada_numero,
        chamada_atendimento_id,
        latitude_cad,
        longitude_cad,
        DATA_HC,
        HORA_HC
    FROM chamada_com_ordenacao
    WHERE rn = 1
),

EMPENHOS AS (
SELECT DISTINCT
emp.chamada_atendimento_id,
CONCAT(emp.tipo_recurso_codigo, emp.viatura_numero_prefixo) AS 'vtr',
emp.empenho_id 
FROM db_bisp_cad_reporting.tb_empenho emp
WHERE 1=1
	AND emp.unidade_servico_codigo_tipo = 'BM'
), 

LIGACOES AS (
SELECT DISTINCT
	atend.chamada_numero,
	FROM_TIMESTAMP(atend.data_hora_inicio_n , 'dd/MM/yyyy') as 'DATA_HA',
	FROM_TIMESTAMP(atend.data_hora_inicio_n , 'HH:mm:ss') AS 'HORA_HA' 
FROM db_bisp_cad_reporting.tb_atendimento atend
WHERE 1=1 
	AND atend.orgao_sigla = 'BM'
	AND atend.chamada_numero IS NOT NULL
),

CHAMADA_ATENDIMENTO AS (
SELECT
    situa.empenho_id,
    FROM_TIMESTAMP(situa.empenho_situacao_data_hora_inicio, 'dd/MM/yyyy') AS DATA_SITUACAO,
    MAX(CASE WHEN situa.empenho_situacao_descricao = 'Despachado' THEN FROM_TIMESTAMP(situa.empenho_situacao_data_hora_inicio, 'HH:mm:ss') ELSE NULL END) AS HORA_D,
    MAX(CASE WHEN situa.empenho_situacao_descricao = 'A caminho' THEN FROM_TIMESTAMP(situa.empenho_situacao_data_hora_inicio, 'HH:mm:ss') ELSE NULL END) AS HORA_HDE,
    MAX(CASE WHEN situa.empenho_situacao_descricao = 'No local' THEN FROM_TIMESTAMP(situa.empenho_situacao_data_hora_inicio, 'HH:mm:ss') ELSE NULL END) AS HORA_HLO
FROM 
    db_bisp_cad_reporting.tb_empenho_situacao situa
WHERE 1=1 
	AND situa.empenho_situacao_codigo IN ('DP', 'NL', 'AC')
GROUP BY 1,2
)

SELECT
	OCORRENCIA.numero_ocorrencia,
	OCORRENCIA.id_unid_resp_registro,
	OCORRENCIA.latitude_reds,
	OCORRENCIA.longitude_reds,
	OCORRENCIA.natureza_codigo,
	OCORRENCIA.codigo_municipio,
	CHAMADA.chamada_numero,
    CHAMADA.latitude_cad,
    CHAMADA.longitude_cad,
	CHAMADA.DATA_HC,
	CHAMADA.HORA_HC,
	EMPENHOS.vtr,
	LIGACOES.DATA_HA,
	LIGACOES.HORA_HA,
	CHAMADA_ATENDIMENTO.DATA_SITUACAO,
	CHAMADA_ATENDIMENTO.HORA_D,
	CHAMADA_ATENDIMENTO.HORA_HDE,
	CHAMADA_ATENDIMENTO.HORA_HLO
FROM OCORRENCIA
LEFT JOIN CHAMADA ON OCORRENCIA.numero_chamada_cad = CHAMADA.chamada_numero
LEFT JOIN EMPENHOS ON CHAMADA.chamada_atendimento_id = EMPENHOS.chamada_atendimento_id
LEFT JOIN LIGACOES ON OCORRENCIA.numero_chamada_cad = LIGACOES.chamada_numero
LEFT JOIN CHAMADA_ATENDIMENTO ON CHAMADA_ATENDIMENTO.empenho_id = EMPENHOS.empenho_id
WHERE CHAMADA.chamada_numero IS NOT NULL;