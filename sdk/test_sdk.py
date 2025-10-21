import tonilsm

db = tonilsm.LSM("test2_db")

db.put(b"tomxx", b"catxx")

db.get("tomxx")
# 'catxx'

t = db.begin_tran(isolation_level=tonilsm.IsolationLevel.READ_COMMITTED)

t.get('tomxx')
# 'catxx'

t.put('tomxx', '1')

t.get('tomxx')
# '1'

db.get("tomxx")
# 'catxx'

t.commit()
# True

db.get("tomxx")
# '1'
