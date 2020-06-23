### ETL PARA PROCESAR DATA SOCIOECONOMICA (POR COMUNA)

import pandas as pd



if __name__ == '__main__':
    #leo casen
    df_casen = pd.read_stata(r'data/Casen 2017.dta', convert_categoricals=False)

    #agrupo informacion por hogar
    df_sel = df_casen.groupby('folio').agg({'ytotcorh': 'mean', 'esc': 'max', 'comuna': 'min', 'expc': 'min'})


    #multiplico por factor de expansion las variables de interés (ingreso y educacion)
    varlist = ['ytotcorh', 'esc']

    for v in varlist:
        df_sel[v+'exp'] = df_sel[v] * df_sel['expc']

    #obtengo estadigrafos por comuna
    df_agg_comuna = df_sel.groupby('comuna').sum()
    for v in varlist:
        df_agg_comuna[v] = df_agg_comuna[v+'exp'] / df_agg_comuna.expc

    df_agg_comuna = df_agg_comuna[varlist]

    df_agg_comuna.reset_index(inplace=True)

    df_pobreza = pd.read_excel(
        r'data/PLANILLA_Estimaciones_comunales_tasa_pobreza_por_ingresos_multidimensional_2017.xlsx', 
        skiprows=2).rename(columns={"Código": "Codigo comuna"})

    df_agg_comuna_plus = df_agg_comuna.merge(df_pobreza, left_on='comuna', right_on='Codigo comuna')

    df_agg_comuna_plus.to_pickle(r'data/data_por_comuna.pkl')
