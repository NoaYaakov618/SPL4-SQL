import repository
from repository import repo


def total_inventory():
    return repo.vaccines.sum_quantity()


def total_demand():
    return repo.clinics.sun_demand()


def total_received():
    return repo.logistics.sun_total_received()


def total_sent():
    return repo.logistics.sun_total_sent()
