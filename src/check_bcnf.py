import pandas as pd
import ast

def get_closure(attr, fds_dict):
    curr_closure = set(attr)
    for lhs, rhs in fds_dict.items():
        if lhs.issubset(attr): # lhs is a subset of attributes
            curr_closure = curr_closure.union(set(lhs)).union(set(rhs))
    return curr_closure

def is_superkey(attr, fds_dict, full_attrset):
    curr_closure = get_closure(attr, fds_dict)
    if full_attrset.issubset(curr_closure):
        return True
    return False


def split_relations(curr_rel, fds_dict, rel_superkey, rel_foreignkey)->list:
    if frozenset(curr_rel) not in rel_superkey:
        rel_superkey[frozenset(curr_rel)] = set()
    if frozenset(curr_rel) not in rel_foreignkey:
        rel_foreignkey[frozenset(curr_rel)] = set()
    for lhs, rhs in fds_dict.items():
        valid_rhs = rhs&curr_rel
        if lhs.issubset(curr_rel) and bool(valid_rhs) : # 只要这个fd在当前关系内，就需要保证它左边是超键。
            if not is_superkey(lhs, fds_dict, curr_rel):
                new_rel_A = lhs|(valid_rhs)
                new_rel_B = curr_rel.difference(valid_rhs)
                if frozenset(new_rel_A) not in rel_foreignkey:
                    rel_foreignkey[frozenset(new_rel_A)] = set()
                if frozenset(new_rel_B) not in rel_foreignkey:
                    rel_foreignkey[frozenset(new_rel_B)] = set()
                for fk in rel_foreignkey[frozenset(curr_rel)]:
                    rel_foreignkey[frozenset(new_rel_A)].add(frozenset(fk&new_rel_A)) # 添加foreign key
                    rel_foreignkey[frozenset(new_rel_B)].add(frozenset(fk&new_rel_B)) # 添加foreign key
                add_new_fk = True
                for fk in rel_foreignkey[frozenset(new_rel_B)]:
                    if lhs.issubset(fk):
                        add_new_fk = False
                        break
                if add_new_fk:
                    rel_foreignkey[frozenset(new_rel_B)].add(frozenset(lhs))
                alist = split_relations(new_rel_A, fds_dict, rel_superkey, rel_foreignkey)
                blist = split_relations(new_rel_B, fds_dict, rel_superkey, rel_foreignkey)
                return alist + blist
            else:
                rel_superkey[frozenset(curr_rel)].add(frozenset(lhs))
    return [curr_rel]

fds = pd.read_csv('/Users/jiangyanbo/working/course/dataPreparation/project/fds_full.csv')
ori_df = pd.read_csv('/Users/jiangyanbo/working/course/dataPreparation/project/Food_Inspections_20240215.csv')
all_attrs = set(ori_df.columns)
fds['lhs'] = fds['lhs'].apply(lambda x: frozenset(ast.literal_eval(x)))
fds['rhs'] = fds['rhs'].apply(lambda x: frozenset([x]))
fds_dict = fds.groupby('lhs')['rhs'].agg(lambda x: frozenset.union(*x)).to_dict()
rel_sk = {}
rel_fk = {}
all_rel = split_relations(all_attrs, fds_dict, rel_sk, rel_fk)
print(all_rel)
print('-----------------super keys ----------------')
for rel in all_rel:
    print(rel, "---------", rel_sk[frozenset(rel)])
    # print("================================================")
print('-----------------foreign keys--------------------')
for rel in all_rel:
    print(rel, "---------", rel_fk[frozenset(rel)])
    # print("================================================")
 

