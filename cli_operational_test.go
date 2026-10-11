package main

import (
 "os"
 "path/filepath"
 "testing"

 "slopdb/db"
)

func TestCLIAtomicImportBackupAndIntegrity(t *testing.T){
 directory:=t.TempDir()
 database,err:=db.Open(filepath.Join(directory,"database"),nil);if err!=nil{t.Fatal(err)}
 previous:=currentDB;currentDB=database
 t.Cleanup(func(){currentDB=previous;_ = database.Close()})
 if _,err=database.Exec("CREATE TABLE imported(id INTEGER PRIMARY KEY,v TEXT UNIQUE)");err!=nil{t.Fatal(err)}
 file:=filepath.Join(directory,"input.csv")
 for _,input:=range []string{"id,v\n1,one\n2,one\n","id,v\n1,one\n\"broken\n"}{
  if err=os.WriteFile(file,[]byte(input),0600);err!=nil{t.Fatal(err)}
  handleImport(file,"imported")
  result,e:=database.Exec("SELECT count(*) AS n FROM imported");if e!=nil||result.Rows[0]["n"].Int!=0{t.Fatal("failed CSV import was partially committed",e,result)}
 }
 if err=os.WriteFile(file,[]byte("id,v\n1,one\n2,two\n"),0600);err!=nil{t.Fatal(err)}
 handleImport(file,"imported")
 handleDotCommand(".integrity")
 backup:=filepath.Join(directory,"backup")
 handleDotCommand(".backup "+backup)
 restored,err:=db.Open(backup,nil);if err!=nil{t.Fatal(err)};defer restored.Close()
 result,err:=restored.Exec("SELECT count(*) AS n FROM imported");if err!=nil||result.Rows[0]["n"].Int!=2{t.Fatal("backup restore",err,result)}
}
