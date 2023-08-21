import sqlite3
import atexit
import sys
import summary
import repository
from repository import repo, _conn


def parse_config(file_name):
    config_file = open(file_name)
    first_line_data = config_file.readline().strip().split(",")
    vaccines_num = int(first_line_data[0])
    supplier_num = int(first_line_data[1])
    clinics_num = int(first_line_data[2])
    logistics_num = int(first_line_data[3])
    config_file_text = config_file.read()

    for line in config_file_text.split("\n"):

        if 0 < vaccines_num:
            data = line.strip().split(",")
            vaccine_id = int(data[0].strip())
            vaccine_date = data[1].strip()
            vaccine_supplier = int(data[2].strip())
            vaccine_quantity = int(data[3].strip())
            vaccine_dto = repository.Vaccine(vaccine_id, vaccine_date, vaccine_supplier, vaccine_quantity)
            repo.vaccines.insert(vaccine_dto)
            vaccines_num = vaccines_num - 1

        elif 0 < supplier_num:
            data = line.strip().split(",")
            supplier_id, supplier_name, supplier_logistics = data
            supplier_dto = repository.Supplier(int(supplier_id), supplier_name, int(supplier_logistics))
            repo.suppliers.insert(supplier_dto)
            supplier_num = supplier_num - 1

        elif 0 < clinics_num:
            data = line.strip().split(",")
            clinics_id, clinics_location, clinics_demand, clinics_logistics = data
            clinics_dto = repository.Clinic(int(clinics_id), clinics_location, int(clinics_demand),
                                            int(clinics_logistics))
            repo.clinics.insert(clinics_dto)
            clinics_num = clinics_num - 1

        elif 0 < logistics_num:
            data = line.strip().split(",")
            logistics_id, logistics_name, count_sent, count_received = data
            logistic_dto = repository.Logistic(int(logistics_id), logistics_name, int(count_sent), int(count_received))
            repo.logistics.insert(logistic_dto)
            logistics_num = logistics_num - 1


def parse_orders(file_name, output):
    with open(output, "w+") as f:
        with open(file_name, "r") as orders_file:
            for line in orders_file:
                order = line.split(",")

                if len(order) == 3:
                    receive_shipment(order[0].strip(), int(order[1].strip()), order[2].strip())
                else:
                    sent_shipment(order[0].strip(), int(order[1].strip()))

                output_line = str(summary.total_inventory()) + ',' + str(summary.total_demand()) + ',' + str(
                    summary.total_received()) + ',' + str(summary.total_sent()) + '\n'
                f.write(output_line)


def receive_shipment(name, amount, date):
    supplier = repo.suppliers.find(name)
    id_supplier = supplier.id
    all_vaccines_table = repo.vaccines.select_all()
    len_of_tuple = len(all_vaccines_table)
    last_vaccine = all_vaccines_table[len_of_tuple - 1]
    vaccine_for_id = repository.Vaccine(*last_vaccine)
    vaccine_id = vaccine_for_id.id + 1
    vaccine_dto = repository.Vaccine(vaccine_id, date, id_supplier, amount)
    repo.vaccines.insert(vaccine_dto)
    new_quantity = repo.logistics.find(supplier.logistic).count_received + amount
    repo.logistics.update_count_received(new_quantity, supplier.logistic)


def sent_shipment(location, amount):
    clinic_from_table = repo.clinics.find(location)
    new_demand = clinic_from_table.demand - amount
    repo.clinics.update_demand(new_demand, location)
    new_count_sent = repo.logistics.find(clinic_from_table.logistic).count_sent + amount
    repo.logistics.update_count_sent(new_count_sent, clinic_from_table.logistic)
    table_ordered_by_date = repo.vaccines.order_by()
    i = 0
    while amount > 0:
        tuple_v = table_ordered_by_date[i]
        curr_vaccine = repository.Vaccine(*tuple_v)

        if curr_vaccine.quantity < amount:
            amount = amount - curr_vaccine.quantity
            date_to_remove = curr_vaccine.date
            repo.vaccines.delete(date_to_remove)
            i = i + 1
        else:
            new_quantity = curr_vaccine.quantity - amount
            repo.vaccines.update_quantity(new_quantity, curr_vaccine.date)
            amount = amount - curr_vaccine.quantity


def print_res():
    tested_db = _conn.execute(
        """SELECT log.name, log.count_sent, log.count_received FROM logistics as log""").fetchall()
    print(tested_db)


def main(args):
    repo.create_tables()
    parse_config(args[1])
    output = args[3]
    parse_orders(args[2], output)


if __name__ == '__main__':
    main(sys.argv)
