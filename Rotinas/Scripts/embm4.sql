WITH 
OCORRENCIA AS (
SELECT 
	ROW_NUMBER() OVER (ORDER BY oco.data_hora_fato ASC) AS ID_CINDS,
	oco.numero_ocorrencia,
	oco.unidade_responsavel_registro_nome,
	oco.natureza_codigo,
	oco.natureza_descricao,
	SUBSTRING(oco.natureza_codigo, 1, 1) AS 'Grupo',
	oco.nome_municipio, 
	oco.numero_latitude,
	oco.numero_longitude,
	oco.codigo_municipio,
	FROM_TIMESTAMP(oco.data_hora_fato, 'dd/MM/yyyy') as 'DataFato',
	oco.id_unid_resp_registro
FROM db_bisp_reds_reporting.tb_ocorrencia oco
WHERE 1=1
	AND YEAR(oco.data_hora_fato) =:ANO_FATO
	AND oco.unidade_responsavel_registro_id_orgao = 2 
	AND SUBSTRING(oco.natureza_codigo, 1, 3) NOT IN ('X01', 'X02', 'X03', 'U30', 'W06', 'X08', 'X99') 
	AND oco.ind_estado IN ('F', 'R')
ORDER BY oco.data_hora_fato ASC
),

VIATURA AS (
SELECT 
	vtr.numero_ocorrencia,
	vtr.placa
FROM db_bisp_reds_reporting.tb_viatura_ocorrencia vtr
WHERE 1=1
	AND YEAR(vtr.data_hora_fato) = :ANO_FATO
	AND vtr.unidade_responsavel_registro_codigo LIKE 'B%'
)

SELECT 
OCORRENCIA.*,
VIATURA.placa
FROM OCORRENCIA
LEFT JOIN VIATURA ON OCORRENCIA.numero_ocorrencia = VIATURA.numero_ocorrencia
ORDER BY OCORRENCIA.ID_CINDS ASC
