import pandas as pd
import ast
import json
from collections import OrderedDict

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


def split_relations(curr_rel, fds_dict, rel_superkey, rel_foreignkey, weak_attrs)->list:
    if frozenset(curr_rel) not in rel_superkey:
        rel_superkey[frozenset(curr_rel)] = set()
    if frozenset(curr_rel) not in rel_foreignkey:
        rel_foreignkey[frozenset(curr_rel)] = set()
    sorted_fds = sorted(fds_dict.items(), key=lambda item: (len(item[0]), -len(item[1])))
    # for lhs, rhs in fds_dict.items():
    for lhs, rhs in sorted_fds:
        valid_rhs = (rhs&curr_rel).difference(set(weak_attrs)) # 当前属性集和当前fd的rhs的交集
        # 这个依赖有效的条件：rhs不是weak attr，lhs没超过3个
        if lhs.issubset(curr_rel) and bool(valid_rhs) and frozenset(valid_rhs) and len(lhs) < 3: # 只要这个fd在当前关系内，就需要保证它左边是超键。
            if not is_superkey(lhs, fds_dict, curr_rel): #如果lhs不是当前属性集的超键，需要分裂
                new_rel_A = lhs|(valid_rhs) # A子属性集
                print('need to split rule:', new_rel_A)
                new_rel_B = curr_rel.difference(valid_rhs) # B子属性集，需要去掉被A拆掉的有效rhs，并且把lhs作为外键。
                if frozenset(new_rel_A) not in rel_foreignkey:
                    rel_foreignkey[frozenset(new_rel_A)] = set()
                if frozenset(new_rel_B) not in rel_foreignkey:
                    rel_foreignkey[frozenset(new_rel_B)] = set()
                for fk in rel_foreignkey[frozenset(curr_rel)]:
                    if bool(fk&new_rel_A):
                        rel_foreignkey[frozenset(new_rel_A)].add(frozenset(fk&new_rel_A)) # 添加foreign key
                    if bool(fk&new_rel_B):
                        rel_foreignkey[frozenset(new_rel_B)].add(frozenset(fk&new_rel_B)) # 添加foreign key
                add_new_fk = True
                for fk in rel_foreignkey[frozenset(new_rel_B)]:
                    if lhs.issubset(fk):
                        add_new_fk = False
                        break
                if add_new_fk:
                    rel_foreignkey[frozenset(new_rel_B)].add(frozenset(lhs))
                alist = split_relations(new_rel_A, fds_dict, rel_superkey, rel_foreignkey, weak_attrs)
                blist = split_relations(new_rel_B, fds_dict, rel_superkey, rel_foreignkey, weak_attrs)
                return alist + blist
            else:
                rel_superkey[frozenset(curr_rel)].add(frozenset(lhs))
    return [curr_rel]

def get_weak_attrs(df):
    all_attrs = df.columns
    weak_attrs = []
    for attr in all_attrs:
        if len(df[attr].drop_duplicates())  <= int(0.001*len(df)):
            weak_attrs.append(attr)
    return weak_attrs

fds = pd.read_csv('/Users/jiangyanbo/working/course/dataPreparation/project/src/fds_combined_cleaned_with_slack.csv')
ori_df = pd.read_csv('/Users/jiangyanbo/working/course/dataPreparation/project/input/combined_cleaning_data_v1.csv', encoding='latin1')
weak_attrs = get_weak_attrs(ori_df)
all_attrs = set(ori_df.columns)
fds['lhs'] = fds['lhs'].apply(lambda x: frozenset(ast.literal_eval(x)))
fds['rhs'] = fds['rhs'].apply(lambda x: frozenset([x]))
fds_dict = fds.groupby('lhs')['rhs'].agg(lambda x: frozenset.union(*x)).to_dict()
dict_str_keys = OrderedDict()

fds_dict = OrderedDict(sorted(fds_dict.items(), key=lambda item: (len(item[0]), -len(item[1]))))

# for key, value in fds_dict.items():
#     # 将frozenset转换为字符串
#     key_str = ', '.join(sorted(key))
#     value_str = ', '.join(sorted(value))
#     # 将转换后的字符串键和原始值存储到新字典中
#     dict_str_keys[key_str] = value_str
# with open('merged_fds.json', 'w') as f:
#     json.dump(dict_str_keys, f, indent=4)

rel_sk = {}
rel_fk = {}
all_rel = split_relations(all_attrs, fds_dict, rel_sk, rel_fk, weak_attrs)
print('-----------------all relations---------------------')
for rel in all_rel:
    print(rel)
print('-----------------super keys ----------------')
for rel in all_rel:
    print(rel, "---------", rel_sk[frozenset(rel)])
    # print("================================================")
print('-----------------foreign keys--------------------')
for rel in all_rel:
    print(rel, "---------", rel_fk[frozenset(rel)])
    # print("================================================")
 

