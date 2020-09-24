# job description
## `import.job`
- invoke bash scripts `import.sh`
## `import.sh`
- generate the hql scripts to fetch current date, and load data of current date into **user_click** table

## `analysis.job`
- invoke bash scripts `analysis.sh`
- dependency = `import.job`

## `analysis.sh`
- fetch the active users of current date from **user_click** table and insert into **user_info** table

# requiremnt
1. assume the diretory of current date already exist