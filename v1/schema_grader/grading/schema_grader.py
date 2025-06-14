from ..embedding.gemini import embed
from ..matching.helpers import cosine_mat, safe_stack
from ..matching.column_matcher import same_type
from scipy.optimize import linear_sum_assignment
import numpy as np

def ser_table(tbl, meta):
    txt = f"TABLE {tbl}: " + ", ".join(f"{c} ({t})" for c, t in meta['cols'])
    if meta['pk']:
        txt += " | PK:" + "_".join(meta['pk'])
    for fk in meta['fks'][:10]:
        txt += f" | FK {fk['parent_tbl']}({ '_'.join(fk['parent_cols']) })->{fk['ref_tbl']}({ '_'.join(fk['ref_cols']) })"
    return txt

def ser_col(tbl, col, typ):
    return f"{tbl}.{col} ({typ})"

def calc_schema_score(answer_schema, student_schema,
                      TBL_TH=0.80, COL_TH=0.82, SCORE_PER_TABLE=0.5):
    ans_tbls = list(answer_schema.keys())
    stu_tbls = list(student_schema.keys())
    # If either schema is empty, return zero score and no table results to avoid stack errors
    if not ans_tbls or not stu_tbls:
        return 0.0, []
    tbl_vec_ans = {t: embed(ser_table(t, m)) for t, m in answer_schema.items()}
    tbl_vec_stu = {t: embed(ser_table(t, m)) for t, m in student_schema.items()}
    A = np.stack([tbl_vec_ans[t] for t in ans_tbls])
    B = np.stack([tbl_vec_stu[t] for t in stu_tbls])
    sim_tbl = A @ B.T
    sim_tbl = sim_tbl / (np.linalg.norm(A, axis=1, keepdims=True) + 1e-8)
    sim_tbl = sim_tbl / (np.linalg.norm(B, axis=1, keepdims=True).T + 1e-8)
    m, n = sim_tbl.shape
    if n < m:
        sim_tbl = np.hstack([sim_tbl, np.zeros((m, m-n))])
    row, col = linear_sum_assignment(-sim_tbl)
    table_pairs = []
    report_rows = []
    for r, c in zip(row, col):
        ans_t = ans_tbls[r]
        if c < n and sim_tbl[r, c] >= TBL_TH:
            table_pairs.append((ans_t, stu_tbls[c], sim_tbl[r, c]))
            report_rows.append([ans_t, stu_tbls[c], sim_tbl[r, c]])
        else:
            report_rows.append([ans_t, "—", 0.0])
    hit_tables = 0
    table_results = []
    total_matched = 0
    for ans_t, stu_t, cos in table_pairs:
        ans_meta = answer_schema[ans_t]
        stu_meta = student_schema[stu_t]
        ans_cols = ans_meta['cols']
        stu_cols = stu_meta['cols']
        if not ans_cols or not stu_cols:
            table_results.append({
                'ans_table': ans_t,
                'stu_table': stu_t,
                'cos': cos,
                'enough_cols': False,
                'pk_ok': False,
                'fk_ok': False,
                'matched_cols': 0
            })
            continue
        vA = safe_stack([embed(ser_col(ans_t, c, d)) for c, d in ans_cols])
        vB = safe_stack([embed(ser_col(stu_t, c, d)) for c, d in stu_cols])
        sim = vA @ vB.T
        sim = sim / (np.linalg.norm(vA, axis=1, keepdims=True) + 1e-8)
        sim = sim / (np.linalg.norm(vB, axis=1, keepdims=True).T + 1e-8)
        m_, n_ = sim.shape
        size = max(m_, n_)
        cost = np.full((size, size), 1e3)
        cost[:m_, :n_] = -sim
        row_, cidx = linear_sum_assignment(cost)
        matched = 0
        for i, j in zip(row_, cidx):
            if i < m_ and j < n_:
                if sim[i, j] >= COL_TH and ans_cols[i][1].lower() == stu_cols[j][1].lower():
                    matched += 1
        ratio_cols = matched / len(ans_cols) if len(ans_cols) else 0
        enough_cols = ratio_cols >= 0.80
        pk_ok = set(ans_meta['pk']) == set(stu_meta['pk'])
        fk_ok = all(any(set(f['parent_cols']) == set(f2['parent_cols']) and set(f['ref_cols']) == set(f2['ref_cols']) and f['ref_tbl'] == f2['ref_tbl'] for f2 in stu_meta['fks']) for f in ans_meta['fks'])
        if enough_cols and pk_ok and fk_ok:
            hit_tables += 1
        table_results.append({
            'ans_table': ans_t,
            'stu_table': stu_t,
            'cos': cos,
            'enough_cols': enough_cols,
            'pk_ok': pk_ok,
            'fk_ok': fk_ok,
            'matched_cols': matched
        })
        total_matched += matched
    total_ans_cols = sum(len(m['cols']) for m in answer_schema.values())
    ratio_all = total_matched / total_ans_cols if total_ans_cols else 0
    schema_score = hit_tables * SCORE_PER_TABLE
    return schema_score, table_results
