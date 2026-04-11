"""
NF-e Preço Líquido — Backend FastAPI
Serve o frontend estático + processa XMLs em memória (sem persistência).
"""
import os, re, json
from io import BytesIO
from typing import Any

import pandas as pd
import xml.etree.ElementTree as ET

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="NF-e Preço Líquido")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Serve o frontend ─────────────────────────────────────────────────────────
HTML_FILE = os.path.join(os.path.dirname(__file__), "nfe_app.html")

@app.get("/", response_class=HTMLResponse)
async def root():
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        return f.read()

# ══════════════════════════════════════════════════════════════════════════════
# HELPERS — PARSER XML
# ══════════════════════════════════════════════════════════════════════════════
NS = re.compile(r'\{[^}]+\}')

def strip_ns(tag: str) -> str:
    return NS.sub('', tag)

def find_text(node, *paths) -> str:
    for path in paths:
        parts = path.split('/')
        cur = [node]
        for part in parts:
            cur = [c for n in cur for c in n if strip_ns(c.tag) == part]
        if cur:
            return (cur[0].text or '').strip()
    return ''

def find_all(node, tag: str):
    return [c for c in node.iter() if strip_ns(c.tag) == tag]

def to_float(s: str) -> float:
    if not s:
        return 0.0
    try:
        return float(s.replace(',', '.'))
    except ValueError:
        return 0.0

def parse_date(s: str) -> str:
    return s[:10] if s and len(s) >= 10 else ''

def parse_nfe(file_bytes: bytes, filename: str) -> list[dict]:
    rows = []
    try:
        tree = ET.fromstring(file_bytes)
    except ET.ParseError:
        return rows

    ide   = find_all(tree, 'ide')
    dest  = find_all(tree, 'dest')
    emit  = find_all(tree, 'emit')
    prot  = find_all(tree, 'infProt')
    infAd = find_all(tree, 'infAdic')

    nNF      = find_text(ide[0],  'nNF')   if ide   else ''
    serie    = find_text(ide[0],  'serie') if ide   else ''
    dhEmi    = parse_date(find_text(ide[0], 'dhEmi')) if ide else ''
    CNPJdest = find_text(dest[0], 'CNPJ')  if dest  else ''
    CNPJemit = find_text(emit[0], 'CNPJ')  if emit  else ''
    nProt    = find_text(prot[0], 'nProt') if prot  else ''
    infCpl   = find_text(infAd[0],'infCpl')if infAd else ''

    chNFe = ''
    for el in tree.iter():
        if strip_ns(el.tag) == 'infNFe':
            chNFe = el.get('Id', '').replace('NFe', '')
            break

    _id_counter = [0]
    for det in find_all(tree, 'det'):
        prod = find_all(det, 'prod')
        imp  = find_all(det, 'imposto')
        p    = prod[0] if prod else det
        nItem = det.get('nItem', '') or ''

        cProd  = find_text(p, 'cProd')
        xProd  = find_text(p, 'xProd')
        NCM    = find_text(p, 'NCM')
        CFOP   = find_text(p, 'CFOP')
        uCom   = find_text(p, 'uCom')
        qCom   = to_float(find_text(p, 'qCom'))
        vUnCom = to_float(find_text(p, 'vUnCom'))
        vProd  = to_float(find_text(p, 'vProd'))
        xPed   = find_text(p, 'xPed')
        nItemPed_s = find_text(p, 'nItemPed')

        if xPed and nItemPed_s:
            nItemPed  = int(nItemPed_s)
            xPednItem = f"{xPed}-{nItemPed}"
        else:
            xPed, nItemPed, xPednItem = '0', 0, '0'

        orig = cst = csticms = ''
        pICMS = bcICMS = vICMS = 0.0
        vBCST = pICMSST = vICMSST = 0.0
        vBCFCPST = pFCPST = vFCPST = pRedBC = 0.0
        bcIPI = pIPI = vIPI = 0.0

        if imp:
            icms_nodes = find_all(imp[0], 'ICMS')
            if icms_nodes:
                for child in icms_nodes[0]:
                    orig     = find_text(child, 'orig')    or orig
                    cst_v    = find_text(child, 'CST')
                    if cst_v: cst = cst_v.zfill(2)
                    pICMS    = to_float(find_text(child, 'pICMS'))    or pICMS
                    bcICMS   = to_float(find_text(child, 'vBC'))      or bcICMS
                    vICMS    = to_float(find_text(child, 'vICMS'))    or vICMS
                    vBCST    = to_float(find_text(child, 'vBCST'))    or vBCST
                    pICMSST  = to_float(find_text(child, 'pICMSST'))  or pICMSST
                    vICMSST  = to_float(find_text(child, 'vICMSST'))  or vICMSST
                    vBCFCPST = to_float(find_text(child, 'vBCFCPST')) or vBCFCPST
                    pFCPST   = to_float(find_text(child, 'pFCPST'))   or pFCPST
                    vFCPST   = to_float(find_text(child, 'vFCPST'))   or vFCPST
                    pRedBC   = to_float(find_text(child, 'pRedBC'))   or pRedBC

            csticms = (orig + cst) if (orig and cst) else 'Simples'
            if not orig: orig = 'Simples'
            if not cst:  cst  = 'Simples'

            ipi_nodes = find_all(imp[0], 'IPI')
            if ipi_nodes:
                for child in ipi_nodes[0]:
                    bcIPI = to_float(find_text(child, 'vBC'))  or bcIPI
                    pIPI  = to_float(find_text(child, 'pIPI')) or pIPI
                    vIPI  = to_float(find_text(child, 'vIPI')) or vIPI

        _id_counter[0] += 1
        rows.append({
            '_id': _id_counter[0],
            'CNPJ Emit': CNPJemit, 'CNPJ Dest': CNPJdest,
            'Data Emissão': dhEmi, 'Nº NF': nNF, 'Série': serie,
            'Chave NF-e': chNFe, 'nProt': nProt,
            'Cód Produto': cProd, 'Descrição': xProd,
            'Ped-Item': xPednItem, 'Pedido': xPed, 'Item Ped': nItemPed,
            'nItem': nItem,
            'NCM': NCM, 'Qtd': qCom, 'UN': uCom,
            'Vl Unit': vUnCom, 'Vl Produto': vProd, 'CFOP': CFOP,
            'CST ICMS': csticms, 'Orig': orig, 'CST': cst,
            '% ICMS': pICMS, 'BC ICMS': bcICMS, 'Vl ICMS': vICMS,
            '% IPI': pIPI, 'BC IPI': bcIPI, 'Vl IPI': vIPI,
            '% ICMS-ST': pICMSST, 'BC ICMS-ST': vBCST, 'Vl ICMS-ST': vICMSST,
            '% FCP-ST': pFCPST, 'BC FCP-ST': vBCFCPST, 'Vl FCP-ST': vFCPST,
            '% Red BC': pRedBC, 'Inf Adicionais': infCpl,
            # campos editáveis (defaults)
            'Fator Conv.': 1.0, 'Multiplicador': 1.0,
            '% PIS+COFINS': None, 'Taxa Câmbio': None, 'Tipo Material': None,
            # campos calculados (preenchidos pelo recalc)
            'Vl Unit BRL': 0.0, 'Vl Unit Pedido': 0.0, 'Qtd Pedido': 0.0,
            'Vl PIS+COFINS': 0.0, 'Preço Líq PC': 0.0, 'Preço Líq Total': 0.0,
        })
    return rows


# ══════════════════════════════════════════════════════════════════════════════
# CÁLCULO PREÇO LÍQUIDO
# ══════════════════════════════════════════════════════════════════════════════
def calcular_linha(row: dict, pis_rate_global: float, taxa_global: float, tipo_global: str) -> dict:
    fator = float(row.get('Fator Conv.') or 1.0) or 1.0
    mult  = float(row.get('Multiplicador') or 1.0) or 1.0
    tipo  = row.get('Tipo Material') or tipo_global
    q_nfe = float(row.get('Qtd') or 1.0) or 1.0

    # Taxa individual sobrepõe global se preenchida
    taxa_row = row.get('Taxa Câmbio')
    taxa = float(taxa_row) if taxa_row is not None else taxa_global

    # PIS individual sobrepõe global se preenchida e > 0
    pis_row = row.get('% PIS+COFINS')
    if pis_row is not None and float(pis_row) > 0:
        pis_rate = float(pis_row) / 100.0
    else:
        pis_rate = pis_rate_global

    vUnit      = float(row.get('Vl Unit') or 0)
    vUnit_ped  = vUnit / fator
    qtd_pedido = q_nfe * fator

    vIPI_raw   = float(row.get('Vl IPI')  or 0)
    vICMS_raw  = float(row.get('Vl ICMS') or 0)
    vIPI_ped   = (vIPI_raw  / q_nfe) / fator
    vICMS_ped  = (vICMS_raw / q_nfe) / fator

    def norm(v):
        v = float(v) if v else 0.0
        return v / 100.0 if v > 1.0 else v

    pICMS = norm(row.get('% ICMS'))
    pIPI  = norm(row.get('% IPI'))

    if tipo == 'Ativo/Consumo':
        bcICMS_ped      = vUnit_ped + vIPI_ped
        vPisCofins      = bcICMS_ped * pis_rate
        conversao_total = (vUnit_ped + vIPI_ped) - vICMS_ped - vIPI_ped - vPisCofins
    else:
        conversao_total = vUnit_ped * (1 - pICMS) * (1 - pis_rate)
        vPisCofins      = vUnit_ped * pis_rate

    preco_liq   = conversao_total / taxa if taxa != 0 else conversao_total
    vUnit_brl   = vUnit / taxa if taxa != 0 else vUnit
    preco_total = preco_liq * mult

    return {
        'Qtd Pedido':     round(qtd_pedido, 2),
        'Vl Unit BRL':    round(vUnit_brl,  2),
        'Vl Unit Pedido': round(vUnit_ped,  2),
        'Vl PIS+COFINS':  round(vPisCofins, 2),
        'Preço Líq PC':   round(preco_liq,  2),
        'Preço Líq Total':round(preco_total,2),
    }


# ══════════════════════════════════════════════════════════════════════════════
# ROTAS API
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/api/upload-xml")
async def upload_xml(files: list[UploadFile] = File(...)):
    all_rows = []
    for f in files:
        content = await f.read()
        all_rows.extend(parse_nfe(content, f.filename))
    if not all_rows:
        raise HTTPException(400, "Nenhum item encontrado nos XMLs enviados.")
    return {"data": all_rows}


class RecalcPayload(BaseModel):
    data: list[dict]
    pis_rate: float = 0.0
    taxa_efetiva: float = 1.0
    tipo_global: str = "Ativo/Consumo"

@app.post("/api/recalc")
async def recalc(payload: RecalcPayload):
    pis_rate = payload.pis_rate / 100.0
    for row in payload.data:
        calc = calcular_linha(row, pis_rate, payload.taxa_efetiva, payload.tipo_global)
        row.update(calc)
    return {"data": payload.data}


@app.post("/api/procv-preview")
async def procv_preview(ref_file: UploadFile = File(...)):
    content = await ref_file.read()
    fname = ref_file.filename.lower()
    try:
        if fname.endswith('.csv'):
            df = pd.read_csv(BytesIO(content))
        else:
            df = pd.read_excel(BytesIO(content), header=0)
        df.columns = df.columns.astype(str).str.strip()
        cols = list(df.columns)
        preview = df.head(5).fillna('').astype(str).to_dict(orient='records')
        return {"columns": cols, "preview": preview}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.post("/api/procv-apply")
async def procv_apply(
    ref_file:   UploadFile = File(...),
    data:       str        = Form(...),
    col_chave:  str        = Form(...),
    col_chave_2:str        = Form(''),
    col_pedido: str        = Form(...),
    col_item:   str        = Form(...),
    campo_xml:  str        = Form('nItem'),
    campo_xml_2:str        = Form(''),
):
    content = await ref_file.read()
    fname = ref_file.filename.lower()
    try:
        if fname.endswith('.csv'):
            df_ref = pd.read_csv(BytesIO(content))
        else:
            df_ref = pd.read_excel(BytesIO(content), header=0)
        df_ref.columns = df_ref.columns.astype(str).str.strip()
    except Exception as e:
        raise HTTPException(400, str(e))

    rows = json.loads(data)

    def _norm(v):
        s = str(v).strip()
        if s.lower() in ('nan','none',''): return ''
        try:
            f = float(s)
            return str(int(f)) if f == int(f) else s
        except ValueError:
            return s

    # Monta lookup: chave1 (+ chave2 opcional) → "PEDIDO-ITEM"
    lookup: dict[tuple, str] = {}
    for _, row in df_ref.iterrows():
        k1 = _norm(row[col_chave])
        k2 = _norm(row[col_chave_2]) if col_chave_2 and col_chave_2 in df_ref.columns else ''
        ped  = _norm(row[col_pedido]).lstrip('0') or '0'
        item = _norm(row[col_item]).lstrip('0')   or '0'
        if k1:
            lookup[(k1, k2)] = f"{ped}-{item}"

    campo_map = {'nItem':'nItem','Cód Produto':'Cód Produto','Descrição':'Descrição','Item Ped':'Item Ped'}
    campo_map2= {'nItem':'nItem','Cód Produto':'Cód Produto','Descrição':'Descrição','Item Ped':'Item Ped'}

    encontrados = nao_encontrados = 0
    for row in rows:
        k1 = _norm(row.get(campo_map.get(campo_xml, campo_xml), ''))
        k2 = _norm(row.get(campo_map2.get(campo_xml_2,''), '')) if campo_xml_2 else ''
        match = lookup.get((k1, k2)) or lookup.get((k1, ''))
        if match:
            row['Ped-Item'] = match
            encontrados += 1
        else:
            nao_encontrados += 1

    return {"data": rows, "encontrados": encontrados, "nao_encontrados": nao_encontrados}


@app.post("/api/confronto-pc")
async def confronto_pc(
    pc_file: UploadFile = File(...),
    data:    str        = Form(...),
    tol_pct: float      = Form(15.0),
    tol_max: float      = Form(300.0),
):
    content = await pc_file.read()
    try:
        df_pc = pd.read_excel(BytesIO(content), header=0)
        df_pc.columns = df_pc.columns.str.strip()
    except Exception as e:
        raise HTTPException(400, str(e))

    COLS_PC = ['Documento','Item','Vl.Líq.Unit.','Aliq.ICMS','Aliq.IPI','Aliq.ST ICMS','NCM','Origem']
    missing = [c for c in COLS_PC if c not in df_pc.columns]
    if missing:
        raise HTTPException(400, f"Colunas não encontradas no PC: {missing}. Disponíveis: {list(df_pc.columns)}")

    df_pc['Chave PC'] = (
        df_pc['Documento'].astype(str).str.strip() + '-' +
        df_pc['Item'].astype(str).str.strip().str.lstrip('0')
    )
    df_pc_key = df_pc.drop_duplicates(subset='Chave PC').set_index('Chave PC')

    rows = json.loads(data)
    result = []

    def safe_pct(v):
        try:
            f = float(v) if v is not None else 0.0
            return round(f * 100, 4) if f <= 1.0 else round(f, 4)
        except: return 0.0

    div_vl = div_icms = div_ipi = div_st = div_ncm = div_orig = sem_match = matches = 0

    for row in rows:
        chave = str(row.get('Ped-Item', '')).strip()
        base = {
            'Descrição':  row.get('Descrição',''),
            'Ped-Item':   chave,
            'ICMS XML (%)': safe_pct(row.get('% ICMS')),
            'IPI XML (%)':  safe_pct(row.get('% IPI')),
            'ICMS-ST XML (%)': safe_pct(row.get('% ICMS-ST')),
            'NCM XML':    str(row.get('NCM','')).strip().replace('.',''),
            'Origem XML': str(row.get('Orig','')).strip(),
            'Preço Líq Total (XML)': row.get('Preço Líq Total', 0),
        }

        if chave in df_pc_key.index:
            matches += 1
            pc = df_pc_key.loc[chave]
            vl_xml = float(row.get('Preço Líq Total') or 0)
            vl_pc  = float(str(pc['Vl.Líq.Unit.']).replace(',','.')) if pd.notna(pc['Vl.Líq.Unit.']) else 0.0
            dif_vl = round(vl_xml - vl_pc, 2)

            lim_tol = min(abs(vl_pc) * tol_pct / 100.0, tol_max) if vl_pc != 0 else tol_max
            dentro  = abs(dif_vl) <= lim_tol
            if abs(dif_vl) <= 0.001: st_dif = 'OK ✅'
            elif dentro:             st_dif = 'TOL ✅'
            else:                    st_dif = 'DIVERGENTE ⚠️'; div_vl += 1

            def cmp(xml_val, col):
                xp = safe_pct(xml_val)
                try: pp = round(float(str(pc[col]).replace(',','.')), 4) if pd.notna(pc[col]) else 0.0
                except: pp = 0.0
                return ('OK ✅' if abs(xp-pp)<0.0001 else 'DIVERGENTE ⚠️'), xp, pp

            st_icms, xi, pi_ = cmp(row.get('% ICMS'),    'Aliq.ICMS')
            st_ipi,  xi2,pi2 = cmp(row.get('% IPI'),     'Aliq.IPI')
            st_st,   xs, ps  = cmp(row.get('% ICMS-ST'), 'Aliq.ST ICMS')

            ncm_xml = str(row.get('NCM','')).strip().replace('.','')
            ncm_pc  = str(pc['NCM']).strip().replace('.','') if pd.notna(pc['NCM']) else ''
            st_ncm  = 'OK ✅' if ncm_xml == ncm_pc else 'DIVERGENTE ⚠️'

            orig_xml = str(row.get('Orig','')).strip()
            orig_pc  = str(pc['Origem']).strip() if pd.notna(pc['Origem']) else ''
            st_orig  = 'OK ✅' if orig_xml == orig_pc else 'DIVERGENTE ⚠️'

            if st_icms != 'OK ✅': div_icms += 1
            if st_ipi  != 'OK ✅': div_ipi  += 1
            if st_ncm  != 'OK ✅': div_ncm  += 1
            if st_orig != 'OK ✅': div_orig  += 1

            result.append({**base,
                'Vl Líq Unit PC': round(vl_pc, 2), 'Dif. Vl Unit': dif_vl,
                'Lim. Tolerância': round(lim_tol,2), 'Status Dif.': st_dif,
                'ICMS PC (%)': pi_, 'Status ICMS': st_icms,
                'IPI PC (%)':  pi2, 'Status IPI':  st_ipi,
                'ICMS-ST PC (%)': ps, 'Status ICMS-ST': st_st,
                'NCM PC': ncm_pc, 'Status NCM': st_ncm,
                'Origem PC': orig_pc, 'Status Origem': st_orig,
                'Encontrado': True,
            })
        else:
            sem_match += 1
            result.append({**base,
                'Vl Líq Unit PC': None, 'Dif. Vl Unit': None,
                'Lim. Tolerância': None, 'Status Dif.': 'SEM MATCH ❌',
                'ICMS PC (%)': None, 'Status ICMS': 'SEM MATCH ❌',
                'IPI PC (%)':  None, 'Status IPI':  'SEM MATCH ❌',
                'ICMS-ST PC (%)': None, 'Status ICMS-ST': 'SEM MATCH ❌',
                'NCM PC': None, 'Status NCM': 'SEM MATCH ❌',
                'Origem PC': None, 'Status Origem': 'SEM MATCH ❌',
                'Encontrado': False,
            })

    kpis = dict(
        matches=matches, sem_match=sem_match,
        div_vl=div_vl, div_icms=div_icms, div_ipi=div_ipi,
        div_ncm=div_ncm, div_orig=div_orig,
    )
    return {"data": result, "kpis": kpis}


class ExportPayload(BaseModel):
    data: list[dict]
    pis_rate: float = 0.0
    taxa_efetiva: float = 1.0
    tipo_global: str = "Ativo/Consumo"

@app.post("/api/export-excel")
async def export_excel(payload: ExportPayload):
    df = pd.DataFrame(payload.data)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='NF-e ICMS')
        ws = writer.sheets['NF-e ICMS']
        ws.insert_rows(1)
        ws['A1'] = (f"Tipo: {payload.tipo_global}  |  PIS+COFINS: {payload.pis_rate:.4f}%  |  "
                    f"Taxa câmbio: {payload.taxa_efetiva:.4f}")
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=relatorio_icms.xlsx"}
    )


class ExportConfrontoPayload(BaseModel):
    data: list[dict]

@app.post("/api/export-confronto")
async def export_confronto(payload: ExportConfrontoPayload):
    df = pd.DataFrame(payload.data)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Confronto PC')
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=confronto_pc.xlsx"}
    )
