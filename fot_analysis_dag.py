"""
DAG для анализа ФОТ (фонд оплаты труда) по проектам
Вариант задания №5

Автор: Давидченко Антонина Сергеевна
Дата: 2025
"""

from datetime import datetime, timedelta
import pandas as pd
import json
import sqlite3
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago

# Конфигурация по умолчанию для DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['test@example.com']
}

# Создание DAG
dag = DAG(
    'projects_fot_analysis',
    default_args=default_args,
    description='Расчет фонда оплаты труда по проектам',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'projects', 'fot', 'variant_5']
)

# Пути к файлам данных
DATA_DIR = '/opt/airflow/dags/data'
DB_PATH = '/opt/airflow/projects_fot.db'


def extract_emplooyes_data(**context):
    """
    Extract: Чтение данных о сотрудниках из CSV файла
    """
    print("Извлечение данных о сотрудниках...")
    csv_path = os.path.join(DATA_DIR, 'employees.csv')

    try:
        employees_df = pd.read_csv(csv_path)
        print(f"Загружено {len(employees_df)} сотрудников")
        print(employees_df.head())

        context['task_instance'].xcom_push(key='employees_data', value=employees_df.to_dict('records'))
        return f"Извлечено {len(employees_df)} сотрудников"

    except Exception as e:
        print(f"Ошибка: {str(e)}")
        raise



def extract_projects_data(**context):
    """
    Extract: Чтение данных о проектах из Excel
    """
    print("Извлечение данных о проектах...")
    excel_path = os.path.join(DATA_DIR, 'projects.xlsx')

    try:
        projects_df = pd.read_excel(excel_path)
        print(f"Загружено {len(projects_df)} записей о проектах")
        print(projects_df.head())

        context['task_instance'].xcom_push(key='projects_data', value=projects_df.to_dict('records'))
        return f"Извлечено {len(projects_df)} записей проектов"

    except Exception as e:
        print(f"Ошибка: {str(e)}")
        raise



def extract_rates_data(**context):
    """
    Extract: Чтение данных о ставках из JSON
    """
    print("Извлечение данных о ставках...")
    json_path = os.path.join(DATA_DIR, 'rates.json')

    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            rates_data = json.load(f)

        print(f"Загружено {len(rates_data)} ставок")
        print(pd.DataFrame(rates_data).head())

        context['task_instance'].xcom_push(key='rates_data', value=rates_data)
        return f"Извлечено {len(rates_data)} ставок"

    except Exception as e:
        print(f"Ошибка: {str(e)}")
        raise



def transform_data(**context):
    """
    Transform: консолидация данных, расчет ФОТ по проектам
    """
    print("Начинаем трансформацию данных...")

    try:                                                                                    
        employees_data = context['task_instance'].xcom_pull(key='employees_data', task_ids='extract_emplooyes')
        projects_data = context['task_instance'].xcom_pull(key='projects_data', task_ids='extract_projects')
        rates_data = context['task_instance'].xcom_pull(key='rates_data', task_ids='extract_rates')

        employees_df = pd.DataFrame(employees_data)
        projects_df = pd.DataFrame(projects_data)
        rates_df = pd.DataFrame(rates_data)

        print("Объединяем данные...")

        merged_df = pd.merge(projects_df, employees_df, on='employee_id', how='left')
        merged_df = pd.merge(merged_df, rates_df, on='position', how='left')

        merged_df['payment'] = merged_df['hours_worked'] * merged_df['rate_per_hour']

        fot_df = merged_df.groupby('project_id').agg(
            total_hours=('hours_worked', 'sum'),
            total_payment=('payment', 'sum')
        ).reset_index()

        print("Результаты по проектам:")
        print(fot_df)

        context['task_instance'].xcom_push(key='fot_stats', value=fot_df.to_dict('records'))
        return f"Проанализировано {len(fot_df)} проектов"

    except Exception as e:
        print(f"Ошибка при трансформации: {str(e)}")
        raise




def load_to_database(**context):
    """
    Load: загрузка ФОТ в SQLite
    """
    print("Загрузка данных в базу данных...")

    try:
        
        fot_stats = context['task_instance'].xcom_pull(
            key='fot_stats', 
            task_ids='transform_data'
            )

        if not fot_stats:
            raise ValueError("Нет данных для загрузки")

     # Создание DataFrame из результатов
        df = pd.DataFrame(fot_stats)
       
        conn = sqlite3.connect(DB_PATH)

        try:
        
            create_table_query="""
            CREATE TABLE IF NOT EXISTS project_fot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                total_hours REAL NOT NULL,
                total_payment REAL NOT NULL,
                analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """


            conn.execute(create_table_query)

            # Очистка таблицы перед загрузкой данных
            conn.execute("DELETE FROM project_fot")

            # Загрузка данных в таблицу
            df.to_sql('project_fot', conn, if_exists='append', index=False)

    
            conn.commit()

            print(f"Успешно загружено {len(df)} записей в базу данных")



            verification_query = "SELECT * FROM project_fot ORDER BY total_payment DESC"

            result = pd.read_sql(verification_query, conn)
            print("Проверка загруженных данных:")
            print(result)

        finally:
            conn.close()


        print("Загрузка в базу данных завершена успешно")
        return f"Загружено {len(df)} записей"

    except Exception as e:
        print(f"Ошибка при загрузке: {str(e)}")
        raise










def generate_report(**context):
    """
    Генерация отчета
    """
    print("Генерация отчета...")

    try:
        conn = sqlite3.connect(DB_PATH)


        try:
            query ="""
            SELECT * 
            FROM project_fot 
            ORDER BY total_payment DESC

            """

            df = pd.read_sql_query(query, conn)
            

            # Формирование отчета
            report = f"""
ОТЧЕТ ПО ФОНДУ ОПЛАТЫ ТРУДА ПО ПРОЕКТАМ
====================================================
Дата анализа: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Всего проектов: {len(df)}

"""

            for _, row in df.iterrows():
                report += f"""
Проект {row['project_id']}:
- общие Часы: {row['total_hours']}
- общий ФОТ: {row['total_payment']:.2f}
"""
             




            print("Отчет сгенерирован:")
            print(report)
            

            # Сохранение отчета в файл
            report_file = '/opt/airflow/project_fot_report.txt'
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"Отчет сохранен в файл: {report_file}")
        

         # Сохранение CSV файла с данными
            csv_file = '/opt/airflow/project_fot_data.csv'
            df.to_csv(csv_file, index=False, encoding='utf-8')
            print(f"Данные сохранены в CSV: {csv_file}")


            # Сохранение данных для email
            context['task_instance'].xcom_push(key='report', value=report)
            context['task_instance'].xcom_push(key='report_file_path', value=report_file)
            context['task_instance'].xcom_push(key='csv_file_path', value=csv_file)
            context['task_instance'].xcom_push(key='result_data', value=df.to_dict('records'))
        finally:
            conn.close()
        return "Отчет успешно создан"

    except Exception as e:
        print(f"Ошибка при создании отчета: {str(e)}")
        raise




extract_emplooyes_task = PythonOperator(
    task_id='extract_emplooyes',
    python_callable=extract_emplooyes_data,
    dag=dag
)

extract_projects_task = PythonOperator(
    task_id='extract_projects',
    python_callable=extract_projects_data,
    dag=dag
)

extract_rates_task = PythonOperator(
    task_id='extract_rates',
    python_callable=extract_rates_data,
    dag=dag
)



# Transform задача
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
    doc_md="""
    ### Трансформация данных
    Объединяет данные из всех источников и ФОТ по проектам.
    """
)

# Load задача
load_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag,
    doc_md="""
    ### Загрузка в базу данных
    Сохраняет результаты анализа в SQLite базу данных.
    """
)


# Генерация отчета
report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
    doc_md="""
    ### Генерация отчета
    Создает детальный отчет с результатами анализа ФОТ по проектам.
    """
)



def send_email_with_attachments(**context):
    """
    Отправка email с файлами отчета
    """
    from airflow.utils.email import send_email
    import os

    try:
        # Получение данных из предыдущих задач
        report = context['task_instance'].xcom_pull(key='report', task_ids='generate_report')
        result_data = context['task_instance'].xcom_pull(key='result_data', task_ids='generate_report')

        html_content = f"""
        <h2> Анализ ФОТ по проектам</h2>

        <h3>информация о выполнении </h3>
        <ul> 
            <li><strong>DAG:</strong> analysis fot </li>
            <li><strong>Дата выполнения:</strong> {context['ds']}</li>
            <li><strong>Статус:</strong> ✅ Все задачи выполнены без ошибок</li>
            <li><strong>Результаты:</strong> Сохранены в базе данных SQLite</li>
        </ul>

        
        <h3>📈 Краткие результаты анализа::</h3>
        <table border="1" style="border-collapse: collapse; width: 100%;">
            <tr style="background-color: #f2f2f2;">
                <th>project_id</th>
                <th>total_hours</th>
                <th>total_payment</th>
            </tr>
        """

        for row in result_data:
            html_content += f"""
            <tr>
                <td>{row['project_id']}</td>
                <td>{row['total_hours']}</td>
                <td>{row['total_payment']:.2f}</td>
            </tr>
            """

        html_content += """
        </table>
        
        <h3>📎 Прикрепленные файлы:</h3>
        <ul>
            <li><strong>project_fot_report.txt</strong> - Подробный текстовый отчет</li>
            <li><strong>project_fot_data.csv</strong> - Данные в формате CSV</li>
        </ul>
        
        <p><em>Детальный отчет также доступен в логах задачи generate_report в Airflow UI.</em></p>
        
        <hr>
        <p style="color: #666; font-size: 12px;">
            Это автоматическое уведомление от системы Apache Airflow<br>
            Время отправки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </p>
        """

  # Подготовка файлов для отправки
        
        report_file = '/opt/airflow/project_fot_report.txt'
        csv_file = '/opt/airflow/project_fot_data.csv'

        files = []

        if os.path.exists(report_file):
            files.append(report_file)
            print(f"Добавлен файл для отправки: {report_file}")
        
        if os.path.exists(csv_file):
            files.append(csv_file)
            print(f"Добавлен файл для отправки: {csv_file}")
        
        send_email(
            to=['test@example.com'],
            subject='📈 ФОТ по проектам — отчет',
            html_content=html_content,
            files=files
        )

        print("Email с результатами и прикрепленными файлами отправлен успешно!")
        return "Email отправлен с прикрепленными файлами"




    except Exception as e:
        print(f"Ошибка email: {str(e)}")
       
        # Отправляем базовое уведомление без файлов
        send_email(
            to=['test@example.com'],
            subject='⚠️ Анализ ФОТ по проектам - Завершен (без файлов)',
            html_content=f"""
            <h3>Анализ ФОТ по проектам завершен успешно!</h3>
            <p>Дата выполнения: {context['ds']}</p>
            <p>Все задачи выполнены без ошибок.</p>
            <p><strong>Примечание:</strong> Файлы результатов не удалось прикрепить из-за ошибки: {str(e)}</p>
            <p>Результаты доступны в логах задачи generate_report.</p>
            """
        )
       
       
       
       
       
        raise




# Email уведомление с файлами
email_task = PythonOperator(
    task_id='send_email_notification',
    python_callable=send_email_with_attachments,
    dag=dag,
    doc_md="""
    ### Отправка email-уведомления
    Отправляет email с результатами анализа и прикрепленными файлами.
    """
)

#  Определение зависимостей между задачами
# Extract задачи выполняются параллельно
[extract_emplooyes_task, extract_projects_task, extract_rates_task] >> transform_task
transform_task >> load_task >> report_task >> email_task 