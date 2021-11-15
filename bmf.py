#data = line[2:10]
#cod = line[12:24]
#abertura = line[56:69]
#fechamento = line[108:121]
#maximo = line[69:82]
#minimo = line[82:95]
#quantidade de negociações = line[147:152]
#quantidade de títulos = line[152:170]

import re
import pathlib
import numpy as np
import sqlite3

currentDirectory = pathlib.Path('.')
filesList = []

for file in currentDirectory.iterdir():
    s = str(file)
    if re.match(r'COTAHIST_.*\.TXT$',s):
        filesList.append(s)
if filesList:
    escolha1 = input("Deseja excluir as ações com baixo volume de negociação?(s/n)\n")
    escolha2 = input("Deseja excluir as ações que não foram negociadas todos os dias?(s/n)\n")
    escolha3 = input("Deseja excluir as ações que sofreram uma variação de mais de 10% em dias consecutivos?(s/n)\n")
else:
    print("Nenhum arquivo de séries históricas encontrado.")
    exit()

for file in filesList:
    f = open(file,"rt")
    d = {}
    for line in f.readlines()[1:-1]:
        cod = line[12:24].rstrip() #tira o espaço contido no final da string
        d2 = d.setdefault(cod,{})
        info = (line[27:39].rstrip(),np.array([int(line[10:12]),int(line[56:69]),int(line[108:121]),int(line[69:82]),int(line[82:95]),\
                                                int(line[95:108]),int(line[147:152]),int(line[152:170]),int(line[170:188])]))
        d2.update({line[2:10]:info})
    f.close()
    
    if escolha1.lower()=='s':
        for a,b in list(d.items()):
            if np.mean([v[1][6] for v in b.values()])<200:
                d.pop(a)
                
    conjunto = set([])
    for d_aux in d.values():
        for k in d_aux.keys():
            conjunto.add(k)
    conjunto = list(conjunto)
    conjunto.sort()
    
    if escolha2.lower()=='s':
        v = {}
        for i in range(len(conjunto)-1):
            for a,b in d.items():
                try:
                    var = b[conjunto[i+1]][1][2]/b[conjunto[i]][1][2] 
                    if var>1.1 or var<0.9:
                        _ = v.setdefault(a,[])
                        v[a].append((conjunto[i],conjunto[i+1]))
                except:
                    pass

        for x in v.keys():
            d.pop(x)
    
    if escolha3.lower()=='s':
        l = [a for a,b in d.items() if set(conjunto)!=set(list(b.keys()))]
        for i in l:
            d.pop(i)
    with sqlite3.connect('SeriesHistoricas.db') as conn:
        c = conn.cursor()
        try:
            c.execute("""CREATE TABLE acoes (
                        id_acao INTEGER  PRIMARY KEY AUTOINCREMENT,
                        cod_neg VARCHAR(12) UNIQUE,
                        nome TEXT);""")
            c.execute("""CREATE TABLE datas (
                        id_data INTEGER PRIMARY KEY AUTOINCREMENT,
                        data_str VARCHAR(8) UNIQUE);""")
            c.execute("""CREATE TABLE cotacoes(
                          id_acao  INTEGER, 
                          id_data  INTEGER, 
                          cod_bdi  INTEGER,
                          preco_abertura  INTEGER,
                          preco_fechamento  INTEGER,
                          preco_max  INTEGER,
                          preco_min  INTEGER,
                          preco_med  INTEGER,
                          vol_neg  INTEGER,
                          vol_acoes  INTEGER,
                          vol_financeiro  INTEGER,
                          CONSTRAINT id_cotacao PRIMARY KEY(id_acao,id_data),
                          FOREIGN KEY(id_acao) REFERENCES acoes(id_acao),
                          FOREIGN KEY(id_data) REFERENCES datas(id_data));""")
                          
            conn.commit()
        except:
            pass
        
  #      try:
 #           ROWS =  c.execute("SELECT * FROM cotacoes WHERE id_acao = id_acao AND id_data = id_data AND cod_bdi=cod_bdi AND preco_abertura=preco_abertura AND preco_fechamento=preco_fechamento AND preco_max=preco_max AND preco_min=preco_min AND preco_med=preco_med AND vol_neg=vol_neg AND vol_acoes=vol_acoes ANDvol_financeiro=vol_financeiro")
#            for row in ROWS:
#                print(row)
#        except:
#            print("erro")
        
        rows = [(u,list(v.values())[0][0]) for (u,v) in d.items()]

        c.executemany("INSERT  OR IGNORE INTO acoes(cod_neg,nome) VALUES (?, ?)", rows)
            
        rows = [(i,) for i in conjunto]
        c.executemany("INSERT  OR IGNORE INTO datas(data_str) VALUES (?)", rows)

               
        try:         
            rows = []
            for (k,v) in d.items():
                c.execute("SELECT id_acao FROM acoes WHERE cod_neg='{}'".format(k))
                id_acao = c.fetchall()[0][0]
    
                for k2,v2 in v.items():
                    c.execute("SELECT id_data FROM datas WHERE data_str='{}'".format(k2))
                    id_data = c.fetchall()[0][0]
                    rows.append((id_acao,id_data,int(v2[1][0]),int(v2[1][1]),int(v2[1][2]),int(v2[1][3]),int(v2[1][4]),\
                                 int(v2[1][5]),int(v2[1][6]),int(v2[1][7]),int(v2[1][8])))
                            
            c.executemany("INSERT OR IGNORE INTO cotacoes (id_acao,id_data,cod_bdi,preco_abertura,preco_fechamento,preco_max,\
                                    preco_min,preco_med,vol_neg,vol_acoes,vol_financeiro)VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows)
            m = c.execute("SELECT * FROM cotacoes")
            #m = c.execute("SELECT id_acao,id_data FROM cotacoes")
            lenn = 0
            aux = 0
            for n in m:
                for j in range(len(rows)):
                    if rows[j][:2] == n[:2]:
                        if rows[j][2:] == n[2:]:
                            pass
                        else:
                            print("erro em '{}':".format(n))
                            pass
#                        for i in l:
#                            lenn = lenn + 1
#                            if rows[j] == i:
#                                lenn=0
                            
#                            else:
#                                aux = aux + 1
#            j = 0
#            for i in l:
#                print(i)
#                print(rows[j])
#                if i == rows[j]:
#                    j=j+1
#                else:
#                    j = 0
#                    print("erro em '{}'".format(i))
#                    break    
#            for i in range(len(rows)):
#                m=c.execute("SELECT * FROM cotacoes WHERE id_acao='{}' AND id_data='{}'".format(rows[i],))
#               # if i[0]==m[0] and i[1]==m[1]:
                #print(i[0])
#            for j in m:
#                print(j)
 
            conn.commit()
        except:
            pass
