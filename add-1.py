f = open('proper_dataset.txt','r')
nf = open('apd.txt','w')

for line in f:

    line = line.replace('\n','')
    nf.write('-1\t'+line+'\n')

f.close()
nf.close()
