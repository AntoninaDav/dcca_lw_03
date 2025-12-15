import pandas as pd
import json
import random
import os

def ensure_data_directory():
    """Создает папку data, если она не существует"""
    data_dir = 'dags/data'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"Создана папка {data_dir}")
    return data_dir




def generate_employees_data(data_dir):
    """Генерация данных о сотрудниках (CSV)"""
    print("Генерация данных о сотрудниках...")
    
    positions = ['manager', 'developer', 'analyst', 'designer', 'qa', 'hr', 'support']
    
    employees_data = {
        'employee_id': list(range(1, 101)),  # 100 сотрудников
        'position': [random.choice(positions) for _ in range(100)]
    }
    
    df = pd.DataFrame(employees_data)
    file_path = os.path.join(data_dir, 'employees.csv')
    df.to_csv(file_path, index=False, encoding='utf-8')
    print(f"Создан файл {file_path} с {len(df)} записями")
    

    return df






def generate_rates_data(data_dir):
    """Генерация ставок по позициям (JSON)"""
    print("Генерация ставок по позициям...")
    
    positions = ['manager', 'developer', 'analyst', 'designer', 'qa', 'hr', 'support']
    rates_data = [{'position': pos, 'rate_per_hour': random.randint(20, 100)} for pos in positions]
    
    file_path = os.path.join(data_dir, 'rates.json')
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(rates_data, f, indent=2, ensure_ascii=False)
    
    print(f"Создан файл {file_path} с {len(rates_data)} позициями")
    return rates_data







def generate_projects_data(employees_df, data_dir):
    """Генерация данных о проектах (Excel)"""
    print("Генерация данных о проектах...")
    
    projects_records = []
    project_id_counter = 1
    
    for employee_id in employees_df['employee_id']:
        num_projects = random.randint(1, 3)  # Каждый сотрудник работает над 1-3 проектами
        for _ in range(num_projects):
            hours_worked = random.randint(5, 40)
            projects_records.append({
                'project_id': project_id_counter,
                'employee_id': employee_id,
                'hours_worked': hours_worked
            })
            project_id_counter += 1
    
    projects_df = pd.DataFrame(projects_records)
    file_path = os.path.join(data_dir, 'projects.xlsx')
    projects_df.to_excel(file_path, index=False, engine='openpyxl')
    print(f"Создан файл {file_path} с {len(projects_df)} записями о проектах")
    
    return projects_df





def generate_statistics(employees_df, rates_data, projects_df, data_dir):
    """Генерация статистики по данным"""
    print("\n📊 СТАТИСТИКА СГЕНЕРИРОВАННЫХ ДАННЫХ:")
    print("=" * 50)
    
    # Статистика по сотрудникам
    position_stats = employees_df['position'].value_counts()
    print(f"Всего сотрудников: {len(employees_df)}")
    print("Распределение по позициям:")
    for pos, count in position_stats.items():
        print(f"  - {pos}: {count} сотрудников")
    
    # Статистика по проектам
    print(f"\nВсего проектов: {projects_df['project_id'].nunique()}")
    avg_hours = projects_df['hours_worked'].mean()
    print(f"Среднее количество часов на проект: {avg_hours:.1f}")
    
    # Расчет зарплат (пример)
    projects_df = projects_df.merge(employees_df, on='employee_id', how='left')
    rates_df = pd.DataFrame(rates_data)
    projects_df = projects_df.merge(rates_df, on='position', how='left')
    projects_df['payment'] = projects_df['hours_worked'] * projects_df['rate_per_hour']
    
    total_payment = projects_df['payment'].sum()
    print(f"\nОбщая сумма выплат сотрудникам: {total_payment:.2f} условных единиц")
    




def main():
    """Основная функция генерации всех данных"""
    print("ЗАПУСК ГЕНЕРАЦИИ ТЕСТОВЫХ ДАННЫХ")
    print("=" * 60)
    
    data_dir = ensure_data_directory()
    
    employees_df = generate_employees_data(data_dir)
    rates_data = generate_rates_data(data_dir)
    projects_df = generate_projects_data(employees_df, data_dir)
    
    generate_statistics(employees_df, rates_data, projects_df, data_dir)
    
    print("\nВСЕ ДАННЫЕ УСПЕШНО СГЕНЕРИРОВАНЫ!")
    print(f"\nСозданные файлы в папке {data_dir}:")
    print("  - employees.csv (данные о сотрудниках)")
    print("  - rates.json (ставки по позициям)")
    print("  - projects.xlsx (данные о проектах)")

if __name__ == "__main__":
    main()