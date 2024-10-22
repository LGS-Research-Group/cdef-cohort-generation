import sys

from cdef_cohort.settings import check_settings

if __name__ == "__main__":
    if check_settings():
        from cdef_cohort.main import main

        main()
    else:
        print("Settings failed to load. Exiting.")
        sys.exit(1)
