from subprocess import call

def evaluateAexp(aexp, aexpontology = "src/main/resources/jsonemailsql/", outputfile = "out.txt"):
    callparams =  ["java", "-jar", "./kql_engine-1.1-SNAPSHOT-jar-with-dependencies.jar", '-kqlq', aexp, '-d',
         '-ont', aexpontology, '-fol', '0', '-out', outputfile, '-u','root', '-p','','-url',"jdbc:mysql://localhost/email_messages"]
    s = call(
       callparams )
    stringcall = ""
    for s in callparams:
        stringcall+=s+" "
    resultstring = ""
    with open('output/'+outputfile) as resultfile:
        for line in resultfile.readlines():
            resultstring += line
    resultstring = resultstring.replace('{', '').replace('}', '').replace("[", '').replace("]", '')
    tables = resultstring.split(' ')[0].strip()
    tables = tables[tables.index(":")+1:].split(',')
    fields = resultstring.split(' ')[1].strip()
    fields = fields[fields.index(":")+1:].split(',')

    return tables, fields
if __name__ == '__main__':
    tables, fields = evaluateAexp("SELECT DISTINCT \ALL*email_address*_:source\ FROM \ALL/emailmessage}\ WHERE ( \ALL*folder\ = 'sent_items' OR \ALL*folder\ = 'sent')")
    print  tables,fields