"""
Tasks e Flows para Giro de Carteira 2025
Nova versão com filtros BBM, sem inadimplência e análise completa
"""

import os
import sys
import warnings
import pandas as pd
import numpy as np
import gc  # Para gestión de memoria
from datetime import datetime, timedelta
from typing import Optional
from prefect import flow, task, get_run_logger

warnings.filterwarnings("ignore")
sys.path.append(r'C:\Git\BI0730')

from libs.geral.utils import *
from libs.geral.myconstants import *


@task(name="configurar_ambiente", log_prints=True)
def configurar_ambiente(ano: int) -> dict:
    """
    Configura ambiente e rotas para processamento
    """
    logger = get_run_logger()
    
    config = {
        'ano': ano,
        'pasta_giro_carteira': r'C:\Git\BI0730\source\rotinas\geral\giro_de_carteira',
        # 'pasta_consultas_sql': r'C:\Git\BI0730\source\rotinas\geral\giro_de_carteira\SQL',  # removido: não usamos mais SQL
        'pasta_pbix': rf'C:\Users\{USER}\Sicredi\TimeBI_0730 - Documentos\01_Rotineiros\33_GiroCarteira',
        'arquivo_saida': rf'C:\Users\{USER}\Sicredi\TimeBI_0730 - Documentos\01_Rotineiros\33_GiroCarteira\giro_de_carteira\giro_de_carteira.parquet',
        'arquivo_atualizacao': rf'C:\Users\{USER}\Sicredi\TimeBI_0730 - Documentos\01_Rotineiros\33_GiroCarteira\giro_de_carteira_atualizacao.parquet',
        'base_inadimplente': os.path.join(PATH_BASES_PARQUET, "base_inadimplentes"),
        'base_associados': os.path.join(PATH_BASES_PARQUET, "associados_total_historico"),
        'base_giro_historico': os.path.join(PATH_BASES_PARQUET, "giro_sicredi_historico.parquet"),
        'filtros_risco_bbm': ["BAIXÍSSIMO", "BAIXO 1", "BAIXO 2", "MÉDIO 1", "MÉDIO 2"],
        'dias_sem_movimentacao': 20,
        'canais_validos': ['AGÊNCIA', 'WHATSAPP', 'MOBI'],
        'excluir_origem_canal': ['RelacionamentoMOBI'],
        'inadimplentes': ['INADIMPLENTE']
    }
    logger.info(f"Ambiente configurado para ano {ano}")
    return config


@task(name="carregar_giro_historico", log_prints=True)
def carregar_giro_historico(config: dict) -> pd.DataFrame:
    """
    Lê parquet giro_sicredi_historico e aplica lógica equivalente ao SQL antigo:
      filtro ano_mes corrente, canais válidos, remove origemcanal proibido,
      mantém MAX(data_ultimo_contato) por cpf_cnpj.
    """
    logger = get_run_logger()
    try:
        caminho = config['base_giro_historico']
        if not os.path.exists(caminho):
            logger.error(f"Parquet de giro não encontrado: {caminho}")
            return pd.DataFrame()
        df = pd.read_parquet(caminho)
        if df.empty:
            logger.warning("Parquet de giro vazio.")
            return pd.DataFrame()
        # Campos obrigatórios para giro de carteira
        colunas_req = {'ano_mes','cpf_cnpj','data_ultimo_contato','origem','canal','origemcanal'}
        # Campos adicionais desejados (não obrigatórios)
        colunas_desejadas = {'categoria', 'cod_faixa_giro', 'des_faixa_giro', 'data_periodo'}
        
        faltantes = colunas_req.difference(df.columns)
        if faltantes:
            logger.error(f"Colunas obrigatórias faltantes no parquet: {faltantes}")
            return pd.DataFrame()
        
        # Verificar colunas desejadas disponíveis
        colunas_disponiveis = colunas_req.union(colunas_desejadas.intersection(df.columns))
        logger.info(f"Colunas disponíveis para giro: {sorted(colunas_disponiveis)}")
        ano_mes_param = int(ano_mes_atual().replace("-",""))
        df['ano_mes'] = pd.to_numeric(df['ano_mes'], errors='coerce')
        df['data_ultimo_contato'] = pd.to_datetime(df['data_ultimo_contato'], errors='coerce')
        df = df.dropna(subset=['data_ultimo_contato'])
        if 'origemcanal' not in df.columns:
            df['origemcanal'] = df['origem'].astype(str) + df['canal'].astype(str)
        filtrado = (df
            .query("ano_mes == @ano_mes_param")
            .loc[lambda d: d['canal'].isin(config['canais_validos'])]
            .loc[lambda d: ~d['origemcanal'].isin(config['excluir_origem_canal'])]
        )
        if filtrado.empty:
            logger.warning("Nenhum registro após filtros.")
            return pd.DataFrame()
        
        # Preparar aggregations para preservar campos importantes
        agg_dict = {'data_ultimo_contato': 'max'}
        
        # Preservar campos de giro quando disponíveis (usando first/last)
        campos_preservar = ['origem', 'categoria', 'canal', 'cod_faixa_giro', 'des_faixa_giro', 'data_periodo', 'origemcanal']
        for campo in campos_preservar:
            if campo in filtrado.columns:
                agg_dict[campo] = 'last'  # Usar último valor por CPF
        
        giro_agg = (filtrado
            .groupby('cpf_cnpj', as_index=False)
            .agg(agg_dict)
            .assign(ano_mes=ano_mes_param)
        )
        logger.info(f"Giro histórico processado: {len(giro_agg)}")
        return giro_agg
    except Exception as e:
        logger.error(f"Erro processamento giro histórico: {e}")
        return pd.DataFrame()


@task(name="carregar_associados_bbm", log_prints=True)
def carregar_associados_bbm(config: dict) -> pd.DataFrame:
    """
    Carrega associados com filtros BBM e sem inadimplência
    """
    logger = get_run_logger()
    
    try:
        # Carregar associados completos
        associados = carregar_associados_completo(com_gestores=True, formatar_telefone=False)
        
        if associados.empty:
            logger.warning("Nenhum associado encontrado")
            return pd.DataFrame()
        
        # Filtros BBM - apenas riscos baixos e médios
        associados_bbm = associados[
            associados['des_faixa_risco'].isin(config['filtros_risco_bbm'])
        ].copy()
        
        # Filtro sem inadimplência (assumindo coluna saldo_devedor)
        if 'saldo_devedor' in associados_bbm.columns:
            associados_bbm = associados_bbm[
                (associados_bbm['saldo_devedor'].isna()) | 
                (associados_bbm['saldo_devedor'] == 0)
            ]
        
        # Filtro sem movimentação espontânea > 20 dias
        if 'dt_ult_mov' in associados_bbm.columns:
            data_limite = datetime.now() - timedelta(days=config['dias_sem_movimentacao'])
            associados_bbm = associados_bbm[
                (pd.to_datetime(associados_bbm['dt_ult_mov']) <= data_limite) |
                (associados_bbm['dt_ult_mov'].isna())
            ]
        
        # Apenas correntistas ativos
        associados_bbm = associados_bbm.query("flg_correntista == 'S' and flg_ativo == 'S'")
        
        logger.info(f"Associados BBM filtrados: {len(associados_bbm)}")
        return associados_bbm
        
    except Exception as e:
        logger.error(f"Erro carregando associados BBM: {e}")
        return pd.DataFrame()


@task(name="carregar_inadimplentes", log_prints=True)
def carregar_inadimplentes(config: dict) -> pd.DataFrame:
    """
    Carrega dados de inadimplentes para análise complementar
    """
    logger = get_run_logger()
    
    try:
        # Tentar carregar base de inadimplentes se existir
        base_inadimplente_path = f"{config['base_inadimplente']}.parquet"
        
        if os.path.exists(base_inadimplente_path):
            inadimplentes_df = pd.read_parquet(base_inadimplente_path)
            
            # Filtros básicos para inadimplentes relevantes
            if not inadimplentes_df.empty:
                # Manter apenas colunas essenciais
                colunas_essenciais = ['cpf_cnpj', 'dias_atraso', 'valor_atraso', 'categoria_inadimplencia']
                colunas_existentes = [col for col in colunas_essenciais if col in inadimplentes_df.columns]
                
                if colunas_existentes:
                    inadimplentes_df = inadimplentes_df[colunas_existentes]
                
                # Marcar como inadimplente
                inadimplentes_df['flg_inadimplente'] = 'S'
                
                logger.info(f"Inadimplentes carregados: {len(inadimplentes_df)}")
                return inadimplentes_df
        
        logger.warning("Base de inadimplentes não encontrada ou vazia")
        return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"Erro carregando inadimplentes: {e}")
        return pd.DataFrame()


@task(name="enriquecer_dados_complementares", log_prints=True)
def enriquecer_dados_complementares(associados_bbm: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquece dados com ISA, MC 6M, score principalidade, PIX, fluxo de caixa
    """
    logger = get_run_logger()
    
    try:
        if associados_bbm.empty:
            return pd.DataFrame()
        
        # Base para enrichment
        df_enriquecido = associados_bbm.copy()
        
        # ISA (relacionamento) - usar função existente se disponível
        try:
            isa_data = trazer_isas_historicos([str(datetime.now().year)], filtrar_ag=False, agencias=[])
            if not isa_data.empty:
                df_enriquecido = df_enriquecido.merge(
                    isa_data[['cpf_cnpj', 'isa']].drop_duplicates(),
                    on='cpf_cnpj',
                    how='left'
                )
        except:
            logger.warning("Não foi possível carregar dados ISA")
        
        # Score principalidade - simulado por enquanto
        df_enriquecido['score_principalidade'] = np.random.randint(1, 11, len(df_enriquecido))
        
        # PIX cadastrado - simulado
        df_enriquecido['pix_cadastrado'] = np.random.choice(['Sim', 'Não'], len(df_enriquecido))
        
        # Fluxo de caixa por segmento
        def definir_fluxo_caixa(row):
            carteira = str(row.get('carteira', ''))
            if carteira.startswith('1'):  # PF
                return np.random.choice(['Sim', 'Não'])  # Recebe salário
            elif carteira.startswith(('2', '3')):  # AG/PJ
                return np.random.choice(['DOMICÍLIO', 'COBRANÇA', 'Nenhum'])
            return 'N/A'
        
        df_enriquecido['fluxo_caixa'] = df_enriquecido.apply(definir_fluxo_caixa, axis=1)
        
        # MC 6M (movimento de conta últimos 6 meses) - simulado
        df_enriquecido['mc_6m'] = np.random.uniform(0, 50000, len(df_enriquecido)).round(2)
        
        logger.info(f"Dados enriquecidos para {len(df_enriquecido)} registros")
        return df_enriquecido
        
    except Exception as e:
        logger.error(f"Erro enriquecendo dados: {e}")
        return associados_bbm


@task(name="processar_giro_carteira", log_prints=True)
def processar_giro_carteira(giro_df: pd.DataFrame, associados_df: pd.DataFrame, config: dict, inadimplentes_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Processa merge entre giro e associados com lógicas de negócio
    """
    logger = get_run_logger()
    
    try:
        if giro_df.empty or associados_df.empty:
            logger.warning("DataFrames vazios para processamento")
            return pd.DataFrame()
        
        # Merge entre giro histórico e associados BBM
        # Preservar todos los campos disponibles del giro
        campos_giro = ['cpf_cnpj', 'data_ultimo_contato', 'ano_mes']
        campos_adicionales_giro = ['origem', 'categoria', 'canal', 'des_faixa_giro', 'data_periodo', 'origemcanal']
        
        # Agregar campos adicionales que estén disponibles en el giro_df
        for campo in campos_adicionales_giro:
            if campo in giro_df.columns:
                campos_giro.append(campo)
        
        logger.info(f"Campos del giro a preservar: {campos_giro}")
        
        resultado = associados_df.merge(
            giro_df[campos_giro],
            on='cpf_cnpj',
            how='left'
        )
        
        # Liberar memoria después del merge principal
        gc.collect()
        
        # Normalização robusta do CPF/CNPJ: limpar não-dígitos e aplicar zfill (otimizada)
        logger.info("Aplicando normalização robusta de CPF/CNPJ...")
        try:
            # Converter para string de forma eficiente
            cpf_str = resultado['cpf_cnpj'].astype(str, copy=False)
            # Limpar não-dígitos
            cpf_clean = cpf_str.str.replace(r'\D', '', regex=True)
            # Aplicar zfill baseado no comprimento
            mask_cnpj = cpf_clean.str.len() > 11
            resultado.loc[mask_cnpj, 'cpf_cnpj'] = cpf_clean.loc[mask_cnpj].str.zfill(14)
            resultado.loc[~mask_cnpj, 'cpf_cnpj'] = cpf_clean.loc[~mask_cnpj].str.zfill(11)
            logger.info("Normalização de CPF/CNPJ concluída")
        except Exception as e:
            logger.warning(f"Erro na normalização de CPF/CNPJ: {e}")
        
        # Eliminar columnas "GA?" e "dat_criacao_registro" se existirem
        colunas_eliminar = [col for col in resultado.columns if 'GA?' in col or col == 'dat_criacao_registro']
        if colunas_eliminar:
            logger.info(f"Eliminando colunas: {colunas_eliminar}")
            resultado = resultado.drop(columns=colunas_eliminar, errors='ignore')
        
        # Garantir coluna ano_mes (referência do giro); se nula, preencher com ano_mes atual
        if 'ano_mes' not in resultado.columns:
            resultado['ano_mes'] = int(ano_mes_atual().replace("-",""))
        else:
            resultado['ano_mes'] = resultado['ano_mes'].fillna(int(ano_mes_atual().replace("-","")))
        
        # Processar datas de giro
        resultado['data_ultimo_giro'] = pd.to_datetime(resultado['data_ultimo_contato'])
        hoy = pd.Timestamp.now()
        resultado['dias_sem_giro'] = resultado['data_ultimo_giro'].apply(
            lambda x: (hoy - x).days if pd.notna(x) else None
        )
        # Nova coluna para referência de atualização (se não existir)
        if 'data_competencia' not in resultado.columns:
            resultado['data_competencia'] = resultado['data_ultimo_giro'].dt.date
        # Categorizar atraso do giro por cores
        def categorizar_atraso_giro(dias):
            if pd.isna(dias):
                return 'Nunca contatado'
            if dias <= 90:
                return 'Em dia'
            if dias <= 180:
                return 'Atenção'
            if dias <= 365:
                return 'Atrasado'
            return 'Giro Vencido'
        
        resultado['categoria_giro'] = resultado['dias_sem_giro'].apply(categorizar_atraso_giro)
        
        # Log dos campos de origem preservados
        campos_origem_preservados = [col for col in ['origem', 'categoria', 'canal', 'cod_faixa_giro', 'des_faixa_giro', 'data_periodo', 'origemcanal'] if col in resultado.columns]
        logger.info(f"Campos de origem do contacto preservados: {campos_origem_preservados}")
        
        # Verificar cobertura dos campos de origem
        for campo in campos_origem_preservados:
            valores_nao_nulos = resultado[campo].notna().sum()
            total = len(resultado)
            percentual = (valores_nao_nulos / total * 100) if total > 0 else 0
            logger.info(f"Campo '{campo}': {valores_nao_nulos}/{total} registros ({percentual:.1f}%)")
        
        # Prioridade para ação (quanto maior, mais prioritário) - versão otimizada
        logger.info("Calculando prioridade de contato de forma otimizada...")
        
        # Inicializar prioridade base
        prioridade = pd.Series(0.0, index=resultado.index)
        
        # Mais dias sem giro = maior prioridade (operação vectorizada)
        mask_com_giro = resultado['dias_sem_giro'].notna()
        prioridade.loc[mask_com_giro] += np.minimum(resultado.loc[mask_com_giro, 'dias_sem_giro'] / 30, 24)
        prioridade.loc[~mask_com_giro] += 25  # Nunca contactado = máxima prioridade
        
        # ISA alto = maior prioridade (se disponível)
        if 'isa' in resultado.columns:
            mask_isa = resultado['isa'].notna()
            prioridade.loc[mask_isa] += resultado.loc[mask_isa, 'isa'] / 10000
        
        # Score principalidade alto = maior prioridade (se disponível)
        if 'score_principalidade' in resultado.columns:
            prioridade += resultado['score_principalidade'].fillna(0)
        
        # Movimentação conta alta = maior prioridade (se disponível)
        if 'mc_6m' in resultado.columns:
            mask_mc = resultado['mc_6m'].notna()
            prioridade.loc[mask_mc] += resultado.loc[mask_mc, 'mc_6m'] / 10000
        
        # Penalizar inadimplentes (menor prioridade)
        if 'flg_inadimplente' in resultado.columns:
            mask_inadimplente = resultado['flg_inadimplente'] == 'S'
            prioridade.loc[mask_inadimplente] -= 5
        
        resultado['prioridade_contato'] = prioridade.round(2)
        resultado = resultado.sort_values('prioridade_contato', ascending=False)
        
        # Liberar memoria después del cálculo de prioridades
        del prioridade
        gc.collect()
        
        resultado['giro_vencido'] = np.where(
            (resultado['dias_sem_giro'].notna()) & (resultado['dias_sem_giro'] >= 366),
            'Sim',
            'Não'
        )
        # Adicionar informações de inadimplência se disponíveis
        if inadimplentes_df is not None and not inadimplentes_df.empty:
            resultado = resultado.merge(
                inadimplentes_df,
                on='cpf_cnpj',
                how='left'
            )
            
            # Marcar inadimplentes
            resultado['flg_inadimplente'] = resultado['flg_inadimplente'].fillna('N')
            
            logger.info(f"Dados de inadimplência mesclados: {len(inadimplentes_df)} registros")
        else:
            # Se não há dados de inadimplentes, marcar todos como não inadimplentes
            resultado['flg_inadimplente'] = 'N'
            logger.info("Nenhum dado de inadimplência disponível")
        
        logger.info(f"Processamento concluído: {len(resultado)} registros")
        return resultado
        
    except Exception as e:
        logger.error(f"Erro processando giro carteira: {e}")
        return pd.DataFrame()


@task(name="salvar_resultado", log_prints=True)
def salvar_resultado(dashboard_df: pd.DataFrame, config: dict) -> bool:
    """
    Salva resultado final em parquet na pasta do Power BI
    """
    logger = get_run_logger()
    
    try:
        if dashboard_df.empty:
            logger.warning("DataFrame vazio - não salvando")
            return False
        
        # Criar pasta se não existir
        os.makedirs(config['pasta_pbix'], exist_ok=True)
        
        # 1. Salvar arquivo principal para dashboard
        dashboard_df.to_parquet(config['arquivo_saida'], index=False)
        logger.info(f"Arquivo principal salvo: {config['arquivo_saida']}")
        
        # 2. Backup com data na mesma pasta
        backup_path = os.path.join(
            config['pasta_pbix'], 
            f'{datetime.now().strftime("%Y%m%d")}giro_carteira.parquet'
        )
        dashboard_df.to_parquet(backup_path, index=False)
        logger.info(f"Backup criado: {backup_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"Erro salvando resultado: {e}")
        return False


@task(name="gerar_parquet_atualizacao", log_prints=True)
def gerar_parquet_atualizacao(dashboard_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Gera parquet de atualização baseado apenas em data_competencia:
      - data_atualizacao = max(data_competencia)
      - proxima_atualizacao = próximo dia útil após hoje
    Retorna o DataFrame de atualização.
    """
    logger = get_run_logger()
    if dashboard_df.empty:
        logger.warning("DataFrame vazio - não gera parquet de atualização")
        return pd.DataFrame()

    if 'data_competencia' not in dashboard_df.columns:
        logger.error("Coluna 'data_competencia' não encontrada no DataFrame final")
        return pd.DataFrame()

    datas = pd.to_datetime(dashboard_df['data_competencia'], errors='coerce')
    data_max = datas.max()
    if pd.isna(data_max):
        logger.error("Não foi possível determinar a data máxima em 'data_competencia'")
        return pd.DataFrame()

    def proximo_dia_util(data_ref: datetime) -> datetime:
        d = data_ref + timedelta(days=1)
        while d.weekday() >= 5:  # 5=sábado, 6=domingo
            d += timedelta(days=1)
        return d

    hoje = datetime.now()
    prox = proximo_dia_util(hoje)

    df_atualizacao = pd.DataFrame([{
        'data_atualizacao': data_max.normalize(),
        'proxima_atualizacao': prox.date(),
        'coluna_referencia': 'data_competencia',
        'gerado_em': hoje
    }])

    caminho = config.get('arquivo_atualizacao')
    if caminho:
        os.makedirs(os.path.dirname(caminho), exist_ok=True)
        df_atualizacao.to_parquet(caminho, index=False)
        logger.info(f"Parquet de atualização salvo: {caminho}")
    else:
        logger.error("Caminho de arquivo de atualização não configurado")
    return df_atualizacao


@flow(name="giro_de_carteira_2025", log_prints=True)
def giro_de_carteira_flow(ano: int = 2025) -> pd.DataFrame:
    """
    Flow principal para Giro de Carteira 2025
    """
    logger = get_run_logger()
    
    logger.info(f"Iniciando Giro de Carteira 2025 - Ano: {ano}")
    
    # Configurar ambiente
    config = configurar_ambiente(ano)
    
    # Carregar dados base
    giro_historico = carregar_giro_historico(config)
    associados_bbm = carregar_associados_bbm(config)
    inadimplentes_df = carregar_inadimplentes(config)  # Novo carregamento de inadimplentes
    
    # Enriquecer com dados complementares
    associados_enriquecidos = enriquecer_dados_complementares(associados_bbm)
    
    # Processar giro de carteira incluindo inadimplentes
    resultado = processar_giro_carteira(giro_historico, associados_enriquecidos, config, inadimplentes_df)
    
    # Salvar resultado
    salvo = salvar_resultado(resultado, config)
    if salvo:
        gerar_parquet_atualizacao(resultado, config)
    else:
        logger.warning("Processamento concluído com problemas na gravação")
    
    return resultado  # adiciona retorno para reutilização em testes


if __name__ == "__main__":
    # Execução local simples: o flow já salva resultado e parquet de atualização internamente
    resultado = giro_de_carteira_flow(ano=2025)
    print(f"Giro de Carteira 2025 processado: {len(resultado)} registros")
