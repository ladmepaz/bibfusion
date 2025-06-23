import pandas as pd

def citation_scopus(df: pd.DataFrame) -> pd.DataFrame:
    """
    Genera un DataFrame de relaciones de citación entre artículos.
    
    Args:
        df: DataFrame que contiene las columnas SR_original y SR_ref
        
    Returns:
        DataFrame con las relaciones de citación (quién citó a quién)
        - SR: Artículo origen (antes SR_original)
        - SR_ref: Artículo citado
    """
    # Verificar que existan las columnas necesarias
    if not all(col in df.columns for col in ['SR_original', 'SR_ref']):
        raise ValueError("El DataFrame debe contener las columnas 'SR_original' y 'SR_ref'")
    
    # Crear el DataFrame de citaciones
    citation_df = df[['SR_original', 'SR_ref']].copy()
    
    # Renombrar columnas según especificaciones
    citation_df = citation_df.rename(columns={'SR_original': 'SR'})
    
    # Eliminar filas donde SR_ref esté vacío (NaN o None)
    citation_df = citation_df.dropna(subset=['SR_ref'])
    
    # Eliminar posibles duplicados (citaciones repetidas)
    citation_df = citation_df.drop_duplicates()
    
    # Resetear índice del DataFrame resultante
    citation_df = citation_df.reset_index(drop=True)
    
    return citation_df