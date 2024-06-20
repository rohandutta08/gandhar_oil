import streamlit as st
import duckdb
import argparse

st.set_page_config(
    page_title="SAP Voucher View", page_icon=":bar-chart:", layout="wide"
)


def get_enum_from_db(sql_query: str) -> list[str]:
    conn.execute(sql_query)
    return [row[0] for row in conn.fetchall()]

db_path = "input/input.duckdb"

conn = duckdb.connect(db_path, read_only=True)

default_doc_types = get_enum_from_db("select distinct blart from bkpf")
if 'blart' in st.query_params:
    selected_doc_types = st.query_params.get_all('blart')
else:
    selected_doc_types = []

default_kalsm = get_enum_from_db("select distinct kalsm from t007s")
if 'kalsm' in st.query_params:
    selected_kalsm = st.query_params['kalsm']
elif 'TAXINN' in default_kalsm:
    selected_kalsm = 'TAXINN'
else:
    selected_kalsm = default_kalsm[0]

if 'voucher' in st.query_params:
    selected_voucher_number = st.query_params['voucher']
else:
    selected_voucher_number = ''


def check_table_exists(table_name):
    try:
        conn.execute(f'select * from {table_name} limit 1')
        conn.fetchone()
        return True
    except:
        return False

prcd_table = "PRCD_ELEMENTS"
if check_table_exists("KONV"):
    prcd_table = "KONV"
else:
    prcd_table = "PRCD_ELEMENTS"

divide_factor = 1.0


def create_in(items):
    if items:
        in_items = [f"'{t}'" for t in items]
        return ",".join(in_items)
    else:
        return "''"

def fetch_dict():
    rows = conn.fetchall()
    columns = [d[0] for d in conn.description]
    output = []
    for row in rows:
        # print(row)
        output.append(dict(zip(columns, row)))
    return output




def show_page():
    st.query_params["blart"] = selected_doc_types
    st.query_params["kalsm"] = selected_kalsm
    st.query_params["voucher"] = selected_voucher_number


    if selected_voucher_number:
        sql = f"select bukrs, gjahr, belnr, awkey, xblnr, blart from bkpf where (belnr LIKE '%{selected_voucher_number}' or awkey LIKE '%{selected_voucher_number}' or xblnr LIKE '%{selected_voucher_number}') AND BLART IN ({create_in(selected_doc_types)})"
    else:
        sql = f"select bukrs, gjahr, belnr, awkey, xblnr, blart from bkpf where blart in ({create_in(selected_doc_types)}) using sample 1000"

    conn.execute(sql)
    doc_row = fetch_dict()

    if doc_row is None or len(doc_row) == 0:
        st.error(f"No document found for - {selected_voucher_number}")
    elif len(doc_row) > 1:
        st.warning(f"Multiple documents found for - {selected_voucher_number}")
        st.dataframe(doc_row, use_container_width=True)
    else:
        (bukrs, gjahr, belnr, awkey, xblnr, blart) = doc_row[0].values()
 
        st.markdown("### BKPF")
        conn.execute(f"select BUKRS, GJAHR, AWKEY, AWTYP, BELNR, BLART, XBLNR, BUDAT, BLDAT, CPUDT, XBLNR, XREVERSAL, TCODE, USNAM from bkpf where bukrs = ? and gjahr = ? and belnr = ?", [bukrs, gjahr, belnr])
        bkpf_row_dict = fetch_dict()
        st.table(bkpf_row_dict)

        bkpf_row = bkpf_row_dict[0]

        st.markdown("### BSEG")
        if 'jspl' in db_path:
            # JSPL has multiple 'KTOPL' in SKAT, but we care about the one that matches this company code
            conn.execute(f"""select
                buzei, koart, buzid, txgrp, shkzg, mwskz, 
                hkont, txt50, sgtxt, REBZG, REBZJ,
                (CASE WHEN shkzg = 'S' THEN dmbtr::float / {divide_factor} ELSE '' END) dmbtr_debit, 
                (CASE WHEN shkzg = 'H' THEN dmbtr::float / {divide_factor} ELSE '' END) dmbtr_credit, 
                bupla,lifnr, kunnr, hsn_sac
                FROM bseg 
                LEFT JOIN T001 ON T001.BUKRS = BSEG.BUKRS
                LEFT OUTER JOIN skat ON bseg.hkont = skat.saknr AND skat.spras = 'E'
            						AND SKAT.KTOPL = T001.KTOPL 
                where bukrs = ? and gjahr = ? and belnr = ?
                and skat.spras = 'E'
                order by shkzg DESC, buzei ASC""", [bukrs, gjahr, belnr])
        else:
            # IRB only has one KTOPL, but it doesn't match BUKRS. There should be a cleaner way to do this but just hard-coding for now
            conn.execute(f"""select
                buzei, koart, buzid, txgrp, shkzg, mwskz, KTOSL, 
                hkont, txt50, sgtxt, REBZG, REBZJ,
                (CASE WHEN shkzg = 'S' THEN dmbtr::float / {divide_factor} ELSE '' END) dmbtr_debit, 
                (CASE WHEN shkzg = 'H' THEN dmbtr::float / {divide_factor} ELSE '' END) dmbtr_credit, 
                bupla, werks, prctr, lifnr, kunnr, gst_part, hsn_sac
                FROM bseg 
                LEFT JOIN T001 ON T001.BUKRS = BSEG.BUKRS
                LEFT OUTER JOIN skat ON bseg.hkont = skat.saknr AND skat.spras = 'E'
            						AND SKAT.KTOPL = T001.KTOPL 
                where BSEG.bukrs = ? and gjahr = ? and belnr = ?
                order by shkzg DESC, buzei ASC""", [bukrs, gjahr, belnr])
        ledger_entries = fetch_dict()
        st.dataframe(ledger_entries, use_container_width=True)


        st.markdown("### BSET")
        conn.execute(f"""select buzei, kschl, KTOSL, txgrp, mwskz, hkont, txt50, hwbas::float / {divide_factor} hwbas, hwste::float / {divide_factor} hwste, kbetr::float / {divide_factor} kbetr
                    from bset
                    left outer join skat on bset.hkont = skat.saknr and skat.spras = 'E'
                    where bukrs = ? and gjahr = ? and belnr = ?
                    order by buzei
                    """, [bukrs, gjahr, belnr])
        tax_entries = fetch_dict()
        st.dataframe(tax_entries, use_container_width=True)

        st.markdown("### Tax Codes")
        tax_codes = set([l['MWSKZ'] for l in ledger_entries] + [t['MWSKZ'] for t in tax_entries])
        conn.execute(f"""SELECT MWSKZ, text1 FROM t007s WHERE spras = 'E' AND kalsm = '{selected_kalsm}' AND mwskz IN ({create_in(tax_codes)})""")
        tax_codes = fetch_dict()
        st.dataframe(tax_codes, use_container_width=True)

        st.markdown("### VBRK")
        conn.execute(f"""select * from vbrk where vbrk.vbeln = ?""", [bkpf_row['AWKEY']])
        vbrk = fetch_dict()
        st.dataframe(vbrk, use_container_width=True)

        if len(vbrk):
            st.markdown("### VBRP")
            vbeln = [r['VBELN'] for r in vbrk][0]
            conn.execute(f"""select * from vbrp  where vbeln = ?""", [vbeln])
            vbrp = fetch_dict()
            st.dataframe(vbrp, use_container_width=True)

            st.markdown("### VBPA")
            conn.execute(f"""select * from vbpa  where vbeln = ?""", [vbeln])
            vbpa = fetch_dict()
            st.dataframe(vbpa, use_container_width=True)

            st.markdown("### PRCD_ELEMENTS / KONV")
            knumv = [r['KNUMV'] for r in vbrk][0]
            query = f"""select p.*, t.vtext from {prcd_table} p join (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY kschl) AS rn from t685t
            ) t on t.kschl = p.kschl where p.knumv = ? and rn = 1 order by kposn"""
            print(query)
            conn.execute(query, [knumv])
            prcd = fetch_dict()
            st.dataframe(prcd, use_container_width=True)

            st.markdown("### SUMMARY OF PRCD_ELEMENTS / KONV")
            knumv = [r['KNUMV'] for r in vbrk][0]
            query = f"""select p.kinak, p.kschl, t.vtext, SUM(p.kwert) kwert_sum, from {prcd_table} p join (
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY kschl) AS rn from t685t
            ) t on t.kschl = p.kschl where p.knumv = ? and rn = 1 group by 1, 2, 3 order by 1, 4 ASC"""
            print(query)
            conn.execute(query, [knumv])
            prcd = fetch_dict()
            st.dataframe(prcd, use_container_width=True)

        st.markdown("### Summary of BSEG")
        conn.execute(f"""select
            hkont, txt50, 
            SUM((CASE WHEN shkzg = 'S' THEN dmbtr::float / {divide_factor} ELSE 0 END))::float dmbtr_debit_sum, 
            SUM((CASE WHEN shkzg = 'H' THEN dmbtr::float / {divide_factor} ELSE 0 END))::float dmbtr_credit_sum
            from bseg 
            LEFT JOIN T001 ON T001.BUKRS = BSEG.BUKRS
            left outer join skat on bseg.hkont = skat.saknr and skat.spras = 'E'
            						AND SKAT.KTOPL = T001.KTOPL
            where BSEG.bukrs = ? and gjahr = ? and belnr = ?
            group by 1, 2
            order by (dmbtr_debit_sum + dmbtr_credit_sum) ASC""", [bukrs, gjahr, belnr])
        ledger_entries_summary = fetch_dict()
        st.dataframe(ledger_entries_summary, use_container_width=True)



        vendors_list = set([l['LIFNR'] for l in ledger_entries])
        try:
            gst_part = set([l['GST_PART'] for l in ledger_entries])
            vendors_union_list = vendors_list.union(gst_part)
        except:
            pass

        st.markdown("### Vendors")
        if len(vendors_union_list):
            conn.execute(f"""select * from lfa1 where lifnr in ({create_in(vendors_union_list)})""")
            vendors = fetch_dict()
            if len(vendors):
                st.table(vendors)
            else:
                st.text("Vendors not found!")
        else:
            st.text("Vendor union list not found!")

        st.markdown("### Customers")
        customers = set([l['KUNNR'] for l in ledger_entries if l['KUNNR']])
        if len(vbrk):
            vbrk_kunrg = set([r['KUNRG'] for r in vbrk])
            vbrk_kunag = set([r['KUNAG'] for r in vbrk])

            customers = customers.union(vbrk_kunrg).union(vbrk_kunag)

        if len(customers):
            conn.execute(f"""select kunnr, name1, stcd3, land1 from kna1 where kunnr in ({create_in(customers)})""")
            customers = fetch_dict()
            st.table(customers)
        else:
            st.text("Customers not found!")
        
        st.markdown("### My GSTIN")
        mygstin = set([r['BUPLA'] for r in ledger_entries ])
        if len(vbrk):
            vbrk_bupla = set([r['BUPLA'] for r in vbrk])
            mygstin = mygstin.union(vbrk_bupla)
            if len(vbrp):
                vbrp_werks = set([r['WERKS'] for r in vbrp])
                mygstin = mygstin.union(vbrp_werks)
        mygstin
        if len(mygstin):
            conn.execute(f"""select l.name1 , l.stcd3  from t001w as j join lfa1 l on l.lifnr = j.lifnr where j.werks in ({create_in(mygstin)})""")
            mygstin = fetch_dict()
            st.table(mygstin)
        else:
            st.text("MyGSTIN not found")

# Select first werks from vbrp 
# union
# Select first bupla from bseg
# union
# Select bupla from vbrk

# Get j1_bbranch where branch is in above list.

with st.sidebar:

    st.text(db_path.split("/")[-1])

    if not selected_doc_types:
        selected_doc_types = default_doc_types

    selected_voucher_number = st.text_input("Voucher Number", help="Either `BELNR` or `AWKEY`", value=selected_voucher_number)

    st.button("Refresh", on_click=show_page)

    with st.expander("Other options"):
        selected_doc_types = st.multiselect("Doc Types", default_doc_types, selected_doc_types)
        selected_kalsm = st.selectbox("KALSM", options=default_kalsm, index=default_kalsm.index(selected_kalsm), help="`KALSM` is the SAP procedure. Typically, it's `TAXINN` or sometimes `ZTAXIN`. Used to fetch tax codes.")