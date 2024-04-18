import pandas as pd
from itertools import combinations
emptyset = frozenset()
'''
Cplus key 是forzenset
Cplus value 是set
Ll key是level int
Ll value 是frozenset
'''
def compute_dependencies(df, Ll, fds, Cplus, slack_diff):
    print('len Ll: ', len(Ll))
    for X in Ll:
        temp = Cplus[emptyset].copy()
        for A in X:
            temp_X = set(X)
            temp_X.remove(A)
            # print('Cplus: ',Cplus)
            # print('temp: ',temp)
            # print('A: ', A)
            # print('Ll: ', Ll)
            # print('X: ', X)
            temp = temp & Cplus[frozenset(temp_X)]
        Cplus[frozenset(X)] = temp.copy()
        # if not Cplus[frozenset(X)] :
        #     print('who : ',frozenset(X))
    #     print('c+ len: ', len(Cplus[frozenset(X)]))
    # print('Cplus num :',len(Cplus))
        # if frozenset({'Address', 'License #'}) in Cplus:
        #     print('Cplus: Address License # : ', Cplus[frozenset({'Address', 'License #'})])
    for X in Ll:
        for A in X & Cplus[frozenset(X)]:
            Complement = set(X)
            Complement.remove(A)
            if len(Complement) == 0:
                continue
            Complement = frozenset(Complement)
            # check if X\A -> A:
            if abs(len(df[list(Complement)].drop_duplicates()) - len(df[list(X)].drop_duplicates())) < slack_diff: # verified
                fds.loc[len(fds)] = {'lhs': list(Complement), 'rhs': A}
                Cplus[frozenset(X)].remove(A)
                TempCplusX = Cplus[frozenset(X)].copy()
                for B in TempCplusX:
                    if B not in X:
                        Cplus[frozenset(X)].remove(B)

def check_super_key(df, X, slack_diff):
    if abs(len(df[list(X)].drop_duplicates()) - len(df.drop_duplicates())) < slack_diff:
        return True
    return False

def prune(df, fds, Ll, Cplus, slack_diff):
    temp_Ll = Ll.copy()
    for X in temp_Ll:
        if not Cplus[X]:
            Ll.remove(X)
            continue
        # check if X is a superkey
        if check_super_key(df, X, slack_diff):
            for A in Cplus[frozenset(X)]:
                if A not in X:
                    temp = Cplus[emptyset].copy()
                    for B in X:
                        temp_X = set(X)
                        temp_X.add(A)
                        temp_X.remove(B)
                        try:
                            temp = temp & Cplus[frozenset(temp_X)] # if key not exists, it is pruned.
                        except KeyError:
                            # print('Cplus keys : ', Cplus.keys())
                            # print(KeyError)
                            pass
                    if A in temp:
                        fds.loc[len(fds)] = {'lhs': list(X), 'rhs':A}
                        # if level1:
                        #     print('X ', X, "\nA: ", A)
            # print('Ll: ', Ll)
            # print('X: ', X)
            Ll.remove(X)

def generate_next_level(Ll, level):
    pair_combinations = combinations(list(Ll), 2)
    candidate_lists = []
    n = 0
    for set1, set2 in pair_combinations:
        candidate_lists.append(set1|set2)
        n += 1
    next_level = set()
    for A in candidate_lists:
        if len(A) == 1+level:
            next_level.add(frozenset(A))
    return next_level

def tane(df, slack_diff):
    all_attrs = df.columns
    fds = pd.DataFrame(columns=['lhs', 'rhs'])
    Cplus={}
    Cplus[emptyset] = set(all_attrs)
    L={}
    L[1] = set(frozenset([i]) for i in all_attrs)     
    for l in range(1, len(all_attrs)): # l from 1 to all
        compute_dependencies(df, L[l], fds, Cplus, slack_diff)
        prune(df, fds, L[l], Cplus, slack_diff) # as easy as possible when start
        L[l+1] = generate_next_level(L[l],l)
        if not L[l+1]:
            break
    return fds
        # for lhs in combinations(all_attrs, length): # 计算所有长度为l的属性组合，相当于Ll

def test_generate_next_level():
    Ll = set([frozenset(['A', 'B', 'C']), frozenset(['A', 'B', 'D']), frozenset(['B','C','D']), frozenset(['B','C','E'])])
    level = 3
    NewL = generate_next_level(Ll, level)
    print(NewL)

# test
if __name__ == '__main__':
    # test_generate_next_level()
    df = pd.read_csv('/Users/jiangyanbo/working/course/dataPreparation/project/Food_Inspections_20240215.csv')
    total_length = len(df)
    slack_diff = int(0.0001*total_length)
    print('slack difference : ', slack_diff)
    fds = tane(df, slack_diff)
    fds.to_csv('fds_full_with_slack.csv')