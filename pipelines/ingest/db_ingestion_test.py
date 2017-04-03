# these modules are needed for the task
import luigi
import psycopg2

class SemanticExploradorCreation(luigi.Task):

	# Checar dependecias.

    def output(self):
        # the output will be a .csv file
        return luigi.LocalTarget("/home/vagrant/data/same_purchases.csv")

    def run(self):
        # these are here for convenience, you'll use
        # environment variables for production code
        host = "localhost"
        database = "vagrantdb"
        user = "vagrant"
        password = "vagrant"

        conn = psycopg2.connect(
            dbname=database,
            user=user,
            host=host,
            password=password)
        cur = conn.cursor()
        cur.execute("""SELECT
          name,
          date,
          price,
          amount
          FROM purchases
        """)
        rows = cur.fetchall()

        with self.output().open("w") as out_file:
            # write a csv header 'by hand'
            out_file.write("name, date, price, amount")
            for row in rows:
                out_file.write("\n")
                # without the :%s, the date will be output in year-month-day format
                # the star before row causes each element to be placed by itself into format
                out_file.write("{}, {:%s}, {}, {}".format(*row))



if __name__ == '__main__':
    luigi.run()
