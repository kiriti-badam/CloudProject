origf = open('data.txt','r')
f = open('new5k.txt','w')
ind = 0
for line in origf:
	if line.startswith('#'):
		continue
	
	(u,v)=line.split('\t')
	u = int(u)
	
	v = v.replace('\n','')
	v = v.replace('\r','')
	
	v = int(v)
	
	if u> 5000 :
		break
		
	if v <= 5000 :
		f.write(str(u)+' '+str(v)+'\n')

f.close()
origf.close()				
