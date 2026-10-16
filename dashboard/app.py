"""
Supply Pulse dashboard: ACCEPTED vendor_items records only (schema "serving", written by services/pg_sink).
Rejected events stay in the Kafka DLQ and quarantined ones in vendor_items.quarantine; neither is shown here.
"""
import os

import pandas as pd
import psycopg
import streamlit as st

st.set_page_config(page_title="Supply Pulse", layout="wide")

DSN = (f"host={os.environ.get('PGHOST', 'localhost')} port={os.environ.get('PGPORT', '5432')} "
       f"dbname={os.environ.get('PGDATABASE', 'qcommerce')} user={os.environ.get('PGUSER', 'qcommerce_user')} "
       f"password={os.environ.get('PGPASSWORD', '')}")


@st.cache_data(ttl=15, show_spinner=False)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    with psycopg.connect(DSN) as conn:
        cur = conn.execute(sql, params)
        cols = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


st.title("Supply Pulse: vendor items")
st.caption("Valid records only. Rejected events stay in the Kafka DLQ (vendor_items.dlq).")

try:
    latest = query("""SELECT vendor_id, item_ref, item_type, item_name, price_egp::float8 AS price_egp, currency,
                             is_available, qty_on_hand, event_time_utc, updated_by
                      FROM serving.vendor_item_latest WHERE NOT is_deleted""")
except Exception as e:                                   # schema not created yet / Postgres down
    st.warning("No data yet. Start the gate and the sink (`make gate`), then replay events.")
    st.caption(str(e).strip().splitlines()[0])
    st.stop()

if latest.empty:
    st.info("serving.vendor_item_latest is empty: the sink has not stored any accepted event yet.")
    st.stop()

# ---------------- sidebar filters
with st.sidebar:
    st.header("Filters")
    if st.button("Refresh"):
        st.cache_data.clear()
    item_type = st.multiselect("Item type", sorted(latest.item_type.unique()), default=sorted(latest.item_type.unique()))
    vendors = st.multiselect("Vendor", sorted(latest.vendor_id.unique()))
    availability = st.radio("Availability", ["All", "Available", "Unavailable"], horizontal=True)
    search = st.text_input("Search item name / ref")

f = latest[latest.item_type.isin(item_type)]
if vendors:
    f = f[f.vendor_id.isin(vendors)]
if availability != "All":
    f = f[f.is_available == (availability == "Available")]
if search:
    s = search.strip().lower()
    f = f[f.item_name.str.lower().str.contains(s, regex=False) | f.item_ref.str.lower().str.contains(s, regex=False)]

# ---------------- KPIs
c1, c2, c3, c4 = st.columns(4)
c1.metric("Valid items", f"{len(f):,}")
c2.metric("Vendors", f"{f.vendor_id.nunique():,}")
c3.metric("Available", f"{f.is_available.mean() * 100:.1f}%" if len(f) else "n/a")
c4.metric("Avg price (EGP)", f"{f.price_egp.mean():.2f}" if len(f) else "n/a")

tab_prices, tab_compare, tab_stock, tab_history = st.tabs(
    ["Latest prices", "Vendor comparison", "Stock & availability", "Price history"])

# ---------------- latest prices
with tab_prices:
    st.dataframe(f.sort_values(["vendor_id", "item_ref"]), use_container_width=True, hide_index=True,
                 column_config={"price_egp": st.column_config.NumberColumn("price (EGP)", format="%.2f")})

# ---------------- vendor comparison: same SKU sold by several vendors
with tab_compare:
    g = f[f.item_type == "grocery"]
    cmp = (g.groupby("item_ref")
             .agg(item_name=("item_name", "first"), vendors=("vendor_id", "nunique"),
                  min_price=("price_egp", "min"), max_price=("price_egp", "max"), avg_price=("price_egp", "mean"))
             .reset_index())
    cmp = cmp[cmp.vendors > 1]
    cmp["spread_pct"] = ((cmp.max_price - cmp.min_price) / cmp.min_price * 100).round(1)
    st.subheader("Products sold by more than one vendor")
    if cmp.empty:
        st.info("No SKU is offered by two or more vendors with the current filters.")
    else:
        st.dataframe(cmp.sort_values("spread_pct", ascending=False), use_container_width=True, hide_index=True)
        pick = st.selectbox("Compare vendors for", cmp.sort_values("spread_pct", ascending=False).item_ref,
                            format_func=lambda r: f"{r}  {cmp.set_index('item_ref').item_name[r]}")
        one = g[g.item_ref == pick].sort_values("price_egp")
        st.bar_chart(one.set_index("vendor_id")["price_egp"])
        st.dataframe(one[["vendor_id", "price_egp", "is_available", "qty_on_hand", "event_time_utc"]],
                     use_container_width=True, hide_index=True)
    st.subheader("Average price per vendor")
    st.bar_chart(f.groupby("vendor_id").price_egp.mean().sort_values(ascending=False).head(30))

# ---------------- stock and availability
with tab_stock:
    a, b = st.columns(2)
    with a:
        st.subheader("Availability by vendor (top 30 by items)")
        av = f.groupby("vendor_id").is_available.agg(items="size", available="sum")
        av["unavailable"] = av["items"] - av["available"]
        st.bar_chart(av.sort_values("items", ascending=False).head(30)[["available", "unavailable"]])
    with b:
        st.subheader("Lowest stock (groceries, in stock)")
        low = f[(f.item_type == "grocery") & f.is_available].sort_values("qty_on_hand").head(25)
        st.dataframe(low[["vendor_id", "item_ref", "item_name", "qty_on_hand", "price_egp"]],
                     use_container_width=True, hide_index=True)
    st.subheader("Out of stock / unavailable")
    st.dataframe(f[~f.is_available][["vendor_id", "item_ref", "item_name", "qty_on_hand", "event_time_utc"]].head(500),
                 use_container_width=True, hide_index=True)

# ---------------- price history (only items that have a history)
with tab_history:
    options = f.sort_values(["vendor_id", "item_ref"])
    if options.empty:
        st.info("No items match the filters.")
    else:
        labels = (options.vendor_id + " | " + options.item_ref + " | " + options.item_name).tolist()
        choice = st.selectbox("Item", labels)
        vid, ref, _ = [x.strip() for x in choice.split("|", 2)]
        hist = query("""SELECT event_time_utc, price_egp::float8 AS price_egp, is_available, qty_on_hand, late_arrival
                        FROM serving.vendor_item_price_history WHERE vendor_id = %s AND item_ref = %s
                        ORDER BY event_time_utc, src_lsn""", (vid, ref))
        if len(hist) < 2:
            st.info("Only one accepted version so far: no price history to plot yet.")
        else:
            st.line_chart(hist.set_index("event_time_utc")["price_egp"])
        st.dataframe(hist, use_container_width=True, hide_index=True)
