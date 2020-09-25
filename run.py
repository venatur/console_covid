from dao_covid import DaoCovid

obj = DaoCovid()
obj.saveChanges()
obj.searching_ext()
obj.download_resource()
path_csv = obj.unzip_resource()
obj.create_db()
obj.copy_data_todb(path_csv)
obj.copy_data_cat()
obj.compareOldNew()
obj.saveChanges()


