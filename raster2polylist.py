import psycopg
from psycopg import sql
from psycopg.pq import Escaping

class Raster2PolyList:
    def __init__(self, databasename, user,host, password):
        self.connect_str = "dbname='" + databasename +  "' user='" + user +  "' host='" + host + "' password='" + password +  "'"

    def convert(self, rasterName, polyName):

        query = sql.SQL('SELECT * FROM process_raster_list()')



        try:
            self.conn = psycopg.connect(self.connect_str)
            self.conn.execute("SET SEARCH_PATH to marxan")
            self.cursor = self.conn.cursor()
            self.cursor.execute(query)
            self.conn.commit
            self.cursor.close()
            self.conn.close()

            #print(full_query)

        except Exception as e:
            print("Can't connect")
            print(e)


    def updateRasterTable(self, rasterNames):
        query = sql.SQL('INSERT INTO marxan.devonrasters (rastername, polyname) VALUES (%s, %s);')

        placeholders = sql.SQL(', ').join(sql.Placeholder() * len(rasterNames[0]))

        isql = sql.SQL("""INSERT INTO devonrasters (rastername, polyname)
        VALUES ({placeholders})""")

        isql = isql.format(placeholders=placeholders)

        with psycopg.connect(self.connect_str) as conn, conn.cursor() as cur:
            print(f'{isql.as_string(conn)=}')
            cur.executemany(isql, rasterNames)
            conn.commit()




    def makePolyName(self, rasterName):
        splitRasterName = rasterName.split("_")
        polyName = splitRasterName[0] + '_poly'
        return(polyName)


    def batchRun(self, rasterList):
        for tile in rasterList:
            print("in batchrun")

            #self.convert(tile.lower(), self.makePolyName(tile.lower()))
            self.updateRasterTable(tile.lower(), self.makePolyName(tile.lower()))

    def makeTRowList(self, rasterList):
        tempList = []
        for tile in rasterList:
            tempList.append([tile.lower(), self.makePolyName(tile.lower())])

        return tempList





def main():
    testList = ['SS21ne_DTM_1m_high_low', 'SS21nw_DTM_1m_high_low', 'SS21se_DTM_1m_high_low']

    list4Devon = ['SS21ne_DTM_1m_high_low', 'SS21nw_DTM_1m_high_low', 'SS21se_DTM_1m_high_low', 'SS21sw_DTM_1m_high_low', 'SS22ne_DTM_1m_high_low', 'SS22nw_DTM_1m_high_low', 'SS22se_DTM_1m_high_low', 'SS22sw_DTM_1m_high_low', 'SS23nw_DTM_1m_high_low', 'SS23se_DTM_1m_high_low', 'SS23sw_DTM_1m_high_low', 'SS30ne_DTM_1m_high_low', 'SS30nw_DTM_1m_high_low', 'SS30se_DTM_1m_high_low', 'SS30sw_DTM_1m_high_low', 'SS31ne_DTM_1m_high_low', 'SS31nw_DTM_1m_high_low', 'SS31se_DTM_1m_high_low', 'SS31sw_DTM_1m_high_low', 'SS32ne_DTM_1m_high_low', 'SS32nw_DTM_1m_high_low', 'SS32se_DTM_1m_high_low', 'SS32sw_DTM_1m_high_low', 'OpenStreetMap', 'ss40ne_dtm_1m_high_low', 'ss40nw_dtm_1m_high_low', 'ss40se_dtm_1m_high_low', 'ss40sw_dtm_1m_high_low', 'ss41ne_dtm_1m_high_low', 'ss41nw_dtm_1m_high_low', 'ss41se_dtm_1m_high_low', 'ss41sw_dtm_1m_high_low', 'ss42ne_dtm_1m_high_low', 'ss42nw_dtm_1m_high_low', 'ss42se_dtm_1m_high_low', 'ss42sw_dtm_1m_high_low', 'ss43ne_dtm_1m_high_low', 'ss43nw_dtm_1m_high_low', 'ss43se_dtm_1m_high_low', 'ss43sw_dtm_1m_high_low', 'ss44ne_dtm_1m_high_low', 'ss44nw_dtm_1m_high_low', 'ss44se_dtm_1m_high_low', 'ss44sw_dtm_1m_high_low', 'ss50ne_dtm_1m_high_low', 'ss50nw_dtm_1m_high_low', 'ss50se_dtm_1m_high_low', 'ss50sw_dtm_1m_high_low', 'ss51ne_dtm_1m_high_low', 'ss51nw_dtm_1m_high_low', 'ss51se_dtm_1m_high_low', 'ss51sw_dtm_1m_high_low', 'ss52ne_dtm_1m_high_low', 'ss52nw_dtm_1m_high_low', 'ss52se_dtm_1m_high_low', 'ss52sw_dtm_1m_high_low', 'ss53ne_dtm_1m_high_low', 'ss53nw_dtm_1m_high_low', 'ss53se_dtm_1m_high_low', 'ss53sw_dtm_1m_high_low', 'ss54ne_dtm_1m_high_low', 'ss54nw_dtm_1m_high_low', 'ss54se_dtm_1m_high_low', 'ss54sw_dtm_1m_high_low', 'ss55se_dtm_1m_high_low', 'ss55sw_dtm_1m_high_low', 'ss60ne_dtm_1m_high_low', 'ss60nw_dtm_1m_high_low', 'ss60se_dtm_1m_high_low', 'ss60sw_dtm_1m_high_low', 'ss61ne_dtm_1m_high_low', 'ss61nw_dtm_1m_high_low', 'ss61se_dtm_1m_high_low', 'ss61sw_dtm_1m_high_low', 'ss62ne_dtm_1m_high_low', 'ss62nw_dtm_1m_high_low', 'ss62se_dtm_1m_high_low', 'ss62sw_dtm_1m_high_low', 'ss63ne_dtm_1m_high_low', 'ss63nw_dtm_1m_high_low', 'ss63se_dtm_1m_high_low', 'ss63sw_dtm_1m_high_low', 'ss64ne_dtm_1m_high_low', 'ss64nw_dtm_1m_high_low', 'ss64se_dtm_1m_high_low', 'ss64sw_dtm_1m_high_low', 'ss65se_dtm_1m_high_low', 'ss65sw_dtm_1m_high_low', 'ss70ne_dtm_1m_high_low', 'ss70nw_dtm_1m_high_low', 'ss70se_dtm_1m_high_low', 'ss70sw_dtm_1m_high_low', 'ss71ne_dtm_1m_high_low', 'ss71nw_dtm_1m_high_low', 'ss71se_dtm_1m_high_low', 'ss71sw_dtm_1m_high_low', 'ss72ne_dtm_1m_high_low', 'ss72nw_dtm_1m_high_low', 'ss72se_dtm_1m_high_low', 'ss72sw_dtm_1m_high_low', 'ss73ne_dtm_1m_high_low', 'ss73nw_dtm_1m_high_low', 'ss73se_dtm_1m_high_low', 'ss73sw_dtm_1m_high_low', 'ss74ne_dtm_1m_high_low', 'ss74nw_dtm_1m_high_low', 'ss74se_dtm_1m_high_low', 'ss74sw_dtm_1m_high_low', 'ss75se_dtm_1m_high_low', 'ss75sw_dtm_1m_high_low', 'ss80ne_dtm_1m_high_low', 'ss80nw_dtm_1m_high_low', 'ss80se_dtm_1m_high_low', 'ss80sw_dtm_1m_high_low', 'ss81ne_dtm_1m_high_low', 'ss81nw_dtm_1m_high_low', 'ss81se_dtm_1m_high_low', 'ss81sw_dtm_1m_high_low', 'ss82ne_dtm_1m_high_low', 'ss82nw_dtm_1m_high_low', 'ss82se_dtm_1m_high_low', 'ss82sw_dtm_1m_high_low', 'ss83nw_dtm_1m_high_low', 'ss83se_dtm_1m_high_low', 'ss83sw_dtm_1m_high_low', 'ss90ne_dtm_1m_high_low', 'ss90nw_dtm_1m_high_low', 'ss90se_dtm_1m_high_low', 'ss90sw_dtm_1m_high_low', 'ss91ne_dtm_1m_high_low', 'ss91nw_dtm_1m_high_low', 'ss91se_dtm_1m_high_low', 'ss91sw_dtm_1m_high_low', 'ss92ne_dtm_1m_high_low', 'ss92nw_dtm_1m_high_low', 'ss92se_dtm_1m_high_low', 'ss92sw_dtm_1m_high_low', 'st00ne_dtm_1m_high_low', 'st00nw_dtm_1m_high_low', 'st00se_dtm_1m_high_low', 'st00sw_dtm_1m_high_low', 'st01ne_dtm_1m_high_low', 'st01nw_dtm_1m_high_low', 'st01se_dtm_1m_high_low', 'st01sw_dtm_1m_high_low', 'st02ne_dtm_1m_high_low', 'st02nw_dtm_1m_high_low', 'st02se_dtm_1m_high_low', 'st02sw_dtm_1m_high_low', 'st10ne_dtm_1m_high_low', 'st10nw_dtm_1m_high_low', 'st10se_dtm_1m_high_low', 'st10sw_dtm_1m_high_low', 'st11ne_dtm_1m_high_low', 'st11nw_dtm_1m_high_low', 'st11se_dtm_1m_high_low', 'st11sw_dtm_1m_high_low', 'st20ne_dtm_1m_high_low', 'st20nw_dtm_1m_high_low', 'st20se_dtm_1m_high_low', 'st20sw_dtm_1m_high_low', 'st21se_dtm_1m_high_low', 'st21sw_dtm_1m_high_low', 'st30nw_dtm_1m_high_low', 'st30se_dtm_1m_high_low', 'st30sw_dtm_1m_high_low', 'sx36ne_dtm_1m_high_low', 'sx36nw_dtm_1m_high_low', 'sx36se_dtm_1m_high_low', 'sx39ne_dtm_1m_high_low', 'sx39nw_dtm_1m_high_low', 'sx39se_dtm_1m_high_low', 'sx39sw_dtm_1m_high_low', 'sx44ne_dtm_1m_high_low', 'sx44nw_dtm_1m_high_low', 'sx45ne_dtm_1m_high_low', 'sx45nw_dtm_1m_high_low', 'sx45se_dtm_1m_high_low', 'sx45sw_dtm_1m_high_low', 'sx46ne_dtm_1m_high_low', 'sx46nw_dtm_1m_high_low', 'sx46se_dtm_1m_high_low', 'sx46sw_dtm_1m_high_low', 'sx47ne_dtm_1m_high_low', 'sx47nw_dtm_1m_high_low', 'sx47se_dtm_1m_high_low', 'sx47sw_dtm_1m_high_low', 'sx48ne_dtm_1m_high_low', 'sx48nw_dtm_1m_high_low', 'sx48se_dtm_1m_high_low', 'sx48sw_dtm_1m_high_low', 'sx49ne_dtm_1m_high_low', 'sx49nw_dtm_1m_high_low', 'sx49se_dtm_1m_high_low', 'sx49sw_dtm_1m_high_low', 'sx54ne_dtm_1m_high_low', 'sx54nw_dtm_1m_high_low', 'sx54se_dtm_1m_high_low', 'sx54sw_dtm_1m_high_low', 'sx55ne_dtm_1m_high_low', 'sx55nw_dtm_1m_high_low', 'sx55se_dtm_1m_high_low', 'sx55sw_dtm_1m_high_low', 'sx56ne_dtm_1m_high_low', 'sx56nw_dtm_1m_high_low', 'sx56se_dtm_1m_high_low', 'sx56sw_dtm_1m_high_low', 'sx57ne_dtm_1m_high_low', 'sx57nw_dtm_1m_high_low', 'sx57se_dtm_1m_high_low', 'sx57sw_dtm_1m_high_low', 'sx58ne_dtm_1m_high_low', 'sx58nw_dtm_1m_high_low', 'sx58se_dtm_1m_high_low', 'sx58sw_dtm_1m_high_low', 'sx59ne_dtm_1m_high_low', 'sx59nw_dtm_1m_high_low', 'sx59se_dtm_1m_high_low', 'sx59sw_dtm_1m_high_low', 'sx63ne_dtm_1m_high_low', 'sx63nw_dtm_1m_high_low', 'sx64ne_dtm_1m_high_low', 'sx64nw_dtm_1m_high_low', 'sx64se_dtm_1m_high_low', 'sx64sw_dtm_1m_high_low', 'sx65ne_dtm_1m_high_low', 'sx65nw_dtm_1m_high_low', 'sx65se_dtm_1m_high_low', 'sx65sw_dtm_1m_high_low', 'sx66ne_dtm_1m_high_low', 'sx66nw_dtm_1m_high_low', 'sx66se_dtm_1m_high_low', 'sx66sw_dtm_1m_high_low', 'sx67ne_dtm_1m_high_low', 'sx67nw_dtm_1m_high_low', 'sx67se_dtm_1m_high_low', 'sx67sw_dtm_1m_high_low', 'sx68ne_dtm_1m_high_low', 'sx68nw_dtm_1m_high_low', 'sx68se_dtm_1m_high_low', 'sx68sw_dtm_1m_high_low', 'sx69ne_dtm_1m_high_low', 'sx69nw_dtm_1m_high_low', 'sx69se_dtm_1m_high_low', 'sx69sw_dtm_1m_high_low', 'sx73ne_dtm_1m_high_low', 'sx73nw_dtm_1m_high_low', 'sx73se_dtm_1m_high_low', 'sx73sw_dtm_1m_high_low', 'sx74ne_dtm_1m_high_low', 'sx74nw_dtm_1m_high_low', 'sx74se_dtm_1m_high_low', 'sx74sw_dtm_1m_high_low', 'sx75ne_dtm_1m_high_low', 'sx75nw_dtm_1m_high_low', 'sx75se_dtm_1m_high_low', 'sx75sw_dtm_1m_high_low', 'sx76ne_dtm_1m_high_low', 'sx76nw_dtm_1m_high_low', 'sx76se_dtm_1m_high_low', 'sx76sw_dtm_1m_high_low', 'sx77ne_dtm_1m_high_low', 'sx77nw_dtm_1m_high_low', 'sx77se_dtm_1m_high_low', 'sx77sw_dtm_1m_high_low', 'sx78ne_dtm_1m_high_low', 'sx78nw_dtm_1m_high_low', 'sx78se_dtm_1m_high_low', 'sx78sw_dtm_1m_high_low', 'sx79ne_dtm_1m_high_low', 'sx79nw_dtm_1m_high_low', 'sx79se_dtm_1m_high_low', 'sx79sw_dtm_1m_high_low', 'sx83nw_dtm_1m_high_low', 'sx83sw_dtm_1m_high_low', 'sx84ne_dtm_1m_high_low', 'sx84nw_dtm_1m_high_low', 'sx84se_dtm_1m_high_low', 'sx84sw_dtm_1m_high_low', 'sx85ne_dtm_1m_high_low', 'sx85nw_dtm_1m_high_low', 'sx85se_dtm_1m_high_low', 'sx85sw_dtm_1m_high_low', 'sx86ne_dtm_1m_high_low', 'sx86nw_dtm_1m_high_low', 'sx86se_dtm_1m_high_low', 'sx86sw_dtm_1m_high_low', 'sx87ne_dtm_1m_high_low', 'sx87nw_dtm_1m_high_low', 'sx87se_dtm_1m_high_low', 'sx87sw_dtm_1m_high_low', 'sx88ne_dtm_1m_high_low', 'sx88nw_dtm_1m_high_low', 'sx88se_dtm_1m_high_low', 'sx88sw_dtm_1m_high_low', 'sx89ne_dtm_1m_high_low', 'sx89nw_dtm_1m_high_low', 'sx89se_dtm_1m_high_low', 'sx89sw_dtm_1m_high_low', 'sx94nw_dtm_1m_high_low', 'sx95ne_dtm_1m_high_low', 'sx95nw_dtm_1m_high_low', 'sx95se_dtm_1m_high_low', 'sx95sw_dtm_1m_high_low', 'sx96ne_dtm_1m_high_low', 'sx96nw_dtm_1m_high_low', 'sx96se_dtm_1m_high_low', 'sx96sw_dtm_1m_high_low', 'sx97ne_dtm_1m_high_low', 'sx97nw_dtm_1m_high_low', 'sx97se_dtm_1m_high_low', 'sx97sw_dtm_1m_high_low', 'sx98ne_dtm_1m_high_low', 'sx98nw_dtm_1m_high_low', 'sx98se_dtm_1m_high_low', 'sx98sw_dtm_1m_high_low', 'sx99ne_dtm_1m_high_low', 'sx99nw_dtm_1m_high_low', 'sx99se_dtm_1m_high_low', 'sx99sw_dtm_1m_high_low', 'sy07ne_dtm_1m_high_low', 'sy07nw_dtm_1m_high_low', 'sy08ne_dtm_1m_high_low', 'sy08nw_dtm_1m_high_low', 'sy08se_dtm_1m_high_low', 'sy08sw_dtm_1m_high_low', 'sy09ne_dtm_1m_high_low', 'sy09nw_dtm_1m_high_low', 'sy09se_dtm_1m_high_low', 'sy09sw_dtm_1m_high_low', 'sy18ne_dtm_1m_high_low', 'sy18nw_dtm_1m_high_low', 'sy18sw_dtm_1m_high_low', 'sy19ne_dtm_1m_high_low', 'sy19nw_dtm_1m_high_low', 'sy19se_dtm_1m_high_low', 'sy19sw_dtm_1m_high_low', 'sy28ne_dtm_1m_high_low', 'sy28nw_dtm_1m_high_low', 'sy29ne_dtm_1m_high_low', 'sy29nw_dtm_1m_high_low', 'sy29se_dtm_1m_high_low', 'sy29sw_dtm_1m_high_low', 'sy38ne_dtm_1m_high_low', 'sy38nw_dtm_1m_high_low', 'sy39ne_dtm_1m_high_low', 'sy39nw_dtm_1m_high_low', 'sy39se_dtm_1m_high_low', 'sy39sw_dtm_1m_high_low']

    raster2poly = Raster2Poly('lnrs', 'postgres', 'localhost', 'outrider77')
    print(len(list4Devon))
    processed_list = raster2poly.makeTRowList(list4Devon)
    print(len(processed_list))
    print(processed_list)
    raster2poly.updateRasterTable(processed_list)


if __name__ == '__main__':
    main()

