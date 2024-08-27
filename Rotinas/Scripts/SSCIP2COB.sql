WITH 
MUNICIPIOS AS (
SELECT
	muni.cdmunicipioibge6,
	CASE 
		WHEN muni.nmrisp LIKE 'Não se aplica' THEN 'Outros estados'
		ELSE LEFT(muni.nmrisp, INSTR(muni.nmrisp, 'RISP') + 3) 
	END AS RISP 
FROM db_bisp_shared.tb_dim_municipio muni
WHERE 1=1
	AND nrlongitude <> 9999
),

OCORRENCIA AS (
SELECT
	ROW_NUMBER() OVER (ORDER BY oco.data_hora_fato ASC) AS ID_CINDS,
	YEAR(oco.data_hora_fato),
	oco.numero_ocorrencia,
	oco.unidade_responsavel_registro_nome,
	SUBSTRING(oco.natureza_codigo, 1, 1) AS 'Grupo',
	SUBSTRING(oco.natureza_codigo, 1, 3) AS 'Classe',
	oco.natureza_codigo,
	oco.natureza_descricao, 
	oco.tipo_logradouro_descricao,
	oco.logradouro_nome,
	oco.numero_endereco,
	oco.descricao_complemento_endereco,
	oco.tipo_logradouro2_descricao,
	oco.logradouro2_nome,
	oco.nome_bairro,
	oco.nome_municipio,
	oco.numero_latitude,
	oco.numero_longitude,
	oco.codigo_municipio,
	MONTH(oco.data_hora_fato) AS 'MesFato',
	FROM_TIMESTAMP(oco.data_hora_fato, 'dd/MM/yyyy') as 'DataFato',
	FROM_TIMESTAMP(oco.data_hora_fato, 'HH:mm:ss') AS 'HoraFato',
	MONTH(oco.data_hora_fim_preenchimento) AS 'MesRegistro',
	FROM_TIMESTAMP(oco.data_hora_fim_preenchimento, 'dd/MM/yyyy') as 'DataRegistro',
	FROM_TIMESTAMP(oco.data_hora_fim_preenchimento, 'HH:mm:ss') AS 'HoraRegistro',
	FROM_TIMESTAMP(oco.data_hora_comunicacao, 'dd/MM/yyyy') as 'DataComunicacao',
	FROM_TIMESTAMP(oco.data_hora_comunicacao, 'HH:mm:ss') AS 'HoraComunicacao',
	FROM_TIMESTAMP(oco.data_hora_local, 'dd/MM/yyyy') as 'DataLocal',
	FROM_TIMESTAMP(oco.data_hora_local, 'HH:mm:ss') AS 'HoraLocal',
	FROM_TIMESTAMP(oco.data_hora_final, 'dd/MM/yyyy') as 'DataFinal',
	FROM_TIMESTAMP(oco.data_hora_final, 'HH:mm:ss') AS 'HoraFinal',
    CASE 
	    WHEN DAYOFWEEK(oco.data_hora_fato) = 1 THEN 'Domingo'
	    WHEN DAYOFWEEK(oco.data_hora_fato) = 2 THEN 'Segunda-feira'
	    WHEN DAYOFWEEK(oco.data_hora_fato) = 3 THEN 'Terça-feira'
	    WHEN DAYOFWEEK(oco.data_hora_fato) = 4 THEN 'Quarta-feira'
	    WHEN DAYOFWEEK(oco.data_hora_fato) = 5 THEN 'Quinta-feira'
	    WHEN DAYOFWEEK(oco.data_hora_fato) = 6 THEN 'Sexta-feira'
	    WHEN DAYOFWEEK(oco.data_hora_fato) = 7 THEN 'Sábado'
	    ELSE '-'
	END AS 'Diasemana',
	oco.relator_matricula,
	oco.relator_cargo ,
	oco.relator_nome,
	oco.relator_nome_unidade,
	oco.historico_ocorrencia, 
	oco.id_unid_resp_registro
FROM db_bisp_reds_reporting.tb_ocorrencia oco
WHERE 1=1
	AND YEAR(oco.data_hora_fato) =:ANO_FATO
	AND SUBSTRING(oco.natureza_codigo, 1, 3) IN ('P01', 'P04') 
	AND oco.ind_estado IN ('F', 'R')
	AND (oco.unidade_responsavel_registro_nome LIKE '12BBM%' 
    OR oco.unidade_responsavel_registro_nome LIKE '12 BBM%' 
    OR oco.unidade_responsavel_registro_nome LIKE '5BBM%' 
    OR oco.unidade_responsavel_registro_nome LIKE '5 BBM%' 
    OR oco.unidade_responsavel_registro_nome LIKE '8BBM%' 
    OR oco.unidade_responsavel_registro_nome LIKE '8 BBM%')
ORDER BY oco.data_hora_fato ASC
),

INTEGRANTE AS (
SELECT 
	inte.numero_ocorrencia,
	COUNT(DISTINCT (inte.numero_matricula)) AS qtde_inte
FROM db_bisp_reds_reporting.tb_integrante_guarnicao_ocorrencia inte
WHERE 1=1
	AND YEAR(inte.data_hora_fato) = :ANO_FATO
GROUP BY 1
),

VIATURA AS (
SELECT 
	vtr.numero_ocorrencia,
	vtr.placa
FROM db_bisp_reds_reporting.tb_viatura_ocorrencia vtr
WHERE 1=1
	AND YEAR(vtr.data_hora_fato) = :ANO_FATO
	AND vtr.unidade_responsavel_registro_codigo LIKE 'B%'
),

PREVENCAO AS (
SELECT
    prev.numero_ocorrencia,
    prev.numero_projeto,
    FROM_TIMESTAMP(prev.data_concessao, 'dd/MM/yyyy') AS data_concessao,
    prev.nome_fantasia,
    GROUP_CONCAT(prev.ocupacao_divisao_descricao, ', ') AS ocupacao_divisao_descricao,
    prev.classificacao_ito21_descricao,
    CASE 
        WHEN prev.id_classif_risco_cor = 9 THEN 'ALTO RISCO'
        WHEN prev.id_classif_risco_cor = 8 THEN 'BAIXO RISCO'
        WHEN prev.id_classif_risco_cor = 7 THEN 'DISPENSADA DE AVCB'
        WHEN prev.id_classif_risco_cor = 10 THEN 'NÍVEL I'
        WHEN prev.id_classif_risco_cor = 11 THEN 'NÍVEL II'
        WHEN prev.id_classif_risco_cor = 12 THEN 'NÍVEL III'
        ELSE '-'
    END AS classif_risco_cor,
    prev.numero_pavimentos,
    prev.vlr_area,
    prev.resultado_prevencao_descricao
FROM db_bisp_reds_reporting.tb_prevencao_vistoria_ocorrencia prev
WHERE 1=1
    AND YEAR(prev.data_hora_fato) = :ANO_FATO
GROUP BY 1,2,3,4,6,7,8,9,10
),

RECIBO AS (
SELECT
	reci.numero_ocorrencia,
	reci.destinatario_matricula,
	reci.nome_cargo_destinatario,
	reci.nome_destinatario
FROM db_bisp_reds_reporting.tb_recibo reci
WHERE 1=1
	AND reci.unidade_responsavel_codigo LIKE 'B%'
	AND YEAR(reci.data_hora_fato) = :ANO_FATO
)

SELECT
	OCORRENCIA.*,
	MUNICIPIOS.RISP,
	INTEGRANTE.qtde_inte,
	VIATURA.placa,
	PREVENCAO.numero_ocorrencia,
	PREVENCAO.numero_projeto,
	PREVENCAO.data_concessao,
	PREVENCAO.nome_fantasia,
	PREVENCAO.ocupacao_divisao_descricao,
	PREVENCAO.classificacao_ito21_descricao,
	PREVENCAO.classif_risco_cor,
	PREVENCAO.numero_pavimentos,
	PREVENCAO.vlr_area,
	PREVENCAO.resultado_prevencao_descricao,
	RECIBO.destinatario_matricula,
	RECIBO.nome_cargo_destinatario,
	RECIBO.nome_destinatario
FROM OCORRENCIA
LEFT JOIN INTEGRANTE ON OCORRENCIA.numero_ocorrencia = INTEGRANTE.numero_ocorrencia
LEFT JOIN VIATURA ON OCORRENCIA.numero_ocorrencia = VIATURA.numero_ocorrencia
LEFT JOIN RECIBO ON OCORRENCIA.numero_ocorrencia = RECIBO.numero_ocorrencia
LEFT JOIN PREVENCAO ON OCORRENCIA.numero_ocorrencia = PREVENCAO.numero_ocorrencia
LEFT JOIN MUNICIPIOS ON OCORRENCIA.codigo_municipio = MUNICIPIOS.cdmunicipioibge6