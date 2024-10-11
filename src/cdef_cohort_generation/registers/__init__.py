from cdef_cohort_generation.registers.akm import process_akm
from cdef_cohort_generation.registers.bef import process_bef
from cdef_cohort_generation.registers.idan import process_idan
from cdef_cohort_generation.registers.ind import process_ind
from cdef_cohort_generation.registers.lpr3_diagnoser import process_lpr3_diagnoser
from cdef_cohort_generation.registers.lpr3_kontakter import process_lpr3_kontakter
from cdef_cohort_generation.registers.lpr_adm import process_lpr_adm
from cdef_cohort_generation.registers.lpr_bes import process_lpr_bes
from cdef_cohort_generation.registers.lpr_diag import process_lpr_diag
from cdef_cohort_generation.registers.uddf import process_uddf

__all__ = [
    "process_akm",
    "process_bef",
    "process_idan",
    "process_ind",
    "process_lpr3_diagnoser",
    "process_lpr3_kontakter",
    "process_lpr_adm",
    "process_lpr_bes",
    "process_lpr_diag",
    "process_uddf",
]
