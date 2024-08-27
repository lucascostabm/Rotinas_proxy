WITH 
OCORRENCIA AS (
SELECT
	oco.numero_ocorrencia,
	FROM_TIMESTAMP(oco.data_hora_fato, 'dd/MM/yyyy') as 'DataFato',
	FROM_TIMESTAMP(oco.data_hora_fato, 'HH:mm:ss') as 'HoraFato',
	oco.natureza_codigo,
	oco.natureza_descricao,
	oco.unidade_responsavel_registro_nome,
	oco.nome_municipio,
	oco.numero_latitude,
	oco.numero_longitude,
	oco.id_unid_resp_registro,
	oco.data_hora_fato
FROM db_bisp_reds_reporting.tb_ocorrencia oco
WHERE 1=1
	AND SUBSTRING(oco.natureza_codigo, 1, 3) NOT IN ('X01', 'X02', 'X03', 'U30', 'W06', 'X08', 'X99')
	AND oco.ind_estado IN ('F', 'R')
	AND YEAR(oco.data_hora_fato) = :ANO_FATO
	AND oco.unidade_responsavel_registro_id_orgao = 2
),

VIATURA AS (
SELECT 
	vtr.numero_ocorrencia,
	vtr.prefixo,
	vtr.numero_registro,
	vtr.placa
FROM db_bisp_reds_reporting.tb_viatura_ocorrencia vtr
WHERE 1=1
	AND YEAR(vtr.data_hora_fato) = :ANO_FATO
	AND vtr.unidade_responsavel_registro_codigo LIKE 'B%'
)

SELECT
	OCORRENCIA.numero_ocorrencia,
	OCORRENCIA.DataFato,
	OCORRENCIA.HoraFato,
	OCORRENCIA.natureza_codigo,
	OCORRENCIA.natureza_descricao,
	OCORRENCIA.unidade_responsavel_registro_nome,
	OCORRENCIA.nome_municipio,
	OCORRENCIA.numero_latitude,
	OCORRENCIA.numero_longitude,
	OCORRENCIA.id_unid_resp_registro,
	VIATURA.prefixo,
	VIATURA.numero_registro,
	VIATURA.placa
FROM OCORRENCIA
LEFT JOIN VIATURA ON OCORRENCIA.numero_ocorrencia = VIATURA.numero_ocorrencia
ORDER BY OCORRENCIA.data_hora_fato ASC

