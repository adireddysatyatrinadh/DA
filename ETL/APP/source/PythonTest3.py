import glob

for name in glob.glob('E:/ETL/APP/output/0004__DL_FACT_REGISTRATION__*.xml'):
    print (name)
    pos=name.index(".")
    print (pos)
    filename=name[:pos]
    fileext=name[pos+1:]
    print(filename,fileext)