f = open('pd.txt','w')
#d = open('degree.txt','w')
origf = open('proper_dataset.txt','r')

ind = 0
list_ind = []

for line in origf:
    
    if line.startswith('#'):
        continue

    #print line
    (u,v)=line.split(' ')

    v = v.replace('\r','')
    v = v.replace('\n','')

    if int(v) >= 5000 :
        continue

#    print (u,v,ind) 
    
    if int(u)==ind :
#        print 'there'
        list_ind.append(v)
    else:
#        print 'why'
        if(len(list_ind)>0):
            str_neighbours = list_ind[0]
            for i in range(1,len(list_ind)):
                str_neighbours=str_neighbours+' '+list_ind[i]

            f.write(str(ind)+' '+str(len(list_ind))+'\n')

#        d.write(str(ind)+' '+str(len(list_ind))+'\n')
        ind = int(u)

        if ind == 5001 : 
            break

        list_ind = []
        list_ind.append(v)

f.close()
origf.close()
