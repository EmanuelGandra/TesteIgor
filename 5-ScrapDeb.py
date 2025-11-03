from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys
import pandas as pd
import time
from selenium.webdriver.common.action_chains import ActionChains

ativos = ['HAPV21','KLBNA2']

service = Service()
driver = webdriver.Chrome(service=service)
driver.maximize_window()
driver.execute_script("document.body.style.zoom='60%'")

df = pd.DataFrame()

try:
    # Acessar o site de login mudando o ativo a cada iteração
    for i in range(len(ativos)):
        # Acessar o site de login com cada ativo da lista
        driver.get(
            f"https://data.anbima.com.br/ferramentas/calculadora/debentures/{ativos[i]}?ativo=debentures")

        # Configura um tempo de espera máximo de 20 segundos
        wait = WebDriverWait(driver, 20)
        driver.execute_script("document.body.style.zoom='60%'")

        try:
            # Verifica se o elemento está presente
            time.sleep(2)
            elemento = driver.find_element(
                By.XPATH, "//p[contains(text(), 'Taxa ANBIMA do ativo')]")

            taxa_anbima_encontrada = True
        except:
            taxa_anbima_encontrada = False

        if taxa_anbima_encontrada:
            # Aguardar até que o botão esteja clicável
            button = wait.until(EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "#card-calcular-precificacao > article > article > section > div > form > div.col-xs-12.precificacao-content__calculate-button.col-no-padding > button")))
            # Clicar no botão
            button.click()
            # Aguarde para garantir que a tabela carregue após o clique
            time.sleep(4)
            # Aguardar a tabela carregar
            table_element = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#card-fluxo-pagamento > article > article > section > div > div > table")))
            print("Tabela carregada com sucesso!")
            # Capturar o conteúdo da tabela com BeautifulSoup
            soup = BeautifulSoup(driver.page_source, "html.parser")
            table = soup.select_one(
                "#card-fluxo-pagamento > article > article > section > div > div > table")
            rows = table.find_all("tr")
            # Make a dataframe WITH THE NAME OF THE ATIVO
            data_list = []
            for row in rows:
                columns = row.find_all("td")
                data = [col.text.strip() for col in columns]
                if data:
                    print(data)
                    data_list.append(data)
                print(ativos[i])

            df_append = pd.DataFrame(data_list)

            # Adiciona o nome do ativo em uma coluna no DataFrame
            df_append["Ativo"] = ativos[i]
            # Concat
            df = pd.concat([df, df_append])

        else:
            driver.get(
                f"https://data.anbima.com.br/debentures/{ativos[i]}/caracteristicas")
            driver.execute_script("document.body.style.zoom='60%'")

            # Configura um tempo de espera máximo de 20 segundos
            wait = WebDriverWait(driver, 20)
            try:
                driver.maximize_window()
                # Localiza o elemento com a classe 'lower-card-item-value'
                taxa_elemento = wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "p.lower-card-item-value")))

                # Extrai o texto do elemento
                taxa_texto = taxa_elemento.text

                # Remove o símbolo de porcentagem e converte para número
                taxa_valor = float(taxa_texto.replace(
                    " %", "").replace(",", "."))

                # Armazena o valor da taxa
                print(f"Taxa ANBIMA encontrada: {taxa_valor}")
                driver.get(
                    f"https://data.anbima.com.br/ferramentas/calculadora/debentures/{ativos[i]}?ativo=debentures")

                # Configura um tempo de espera máximo de 20 segundos
                wait = WebDriverWait(driver, 20)
                driver.execute_script("document.body.style.zoom='60%'")

                # Aguardar até que o botão esteja clicável
                # Formata a taxa para o formato com vírgula (como no exemplo do campo)
                taxa_formatada = str(f"{taxa_valor:.6f}").replace(".", ",")

                # Localiza o campo de entrada pelo seletor CSS da classe 'anbima-ui-input__input'
                # input_elemento = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "input.anbima-ui-input__input")))
                input_elemento = wait.until(EC.presence_of_element_located(
                    (By.XPATH, "//div[@id='precificacao-input-taxa']//input")
                ))

                driver.execute_script("""
                    const input = arguments[0];
                    const valor = arguments[1];
                    input.value = valor;
                    input.dispatchEvent(new Event('input', { bubbles: true }));
                    input.dispatchEvent(new Event('change', { bubbles: true }));
                """, input_elemento, taxa_formatada)
                input_elemento.send_keys(Keys.ENTER)
                button = wait.until(EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "#card-calcular-precificacao > article > article > section > div > form > div.col-xs-12.precificacao-content__calculate-button.col-no-padding > button")))
                # Clicar no botão
                button.click()
                # Aguarde para garantir que a tabela carregue após o clique
                time.sleep(4)

                # Aguardar a tabela carregar
                table_element = wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "#card-fluxo-pagamento > article > article > section > div > div > table")))
                print("Tabela carregada com sucesso!")

                # Capturar o conteúdo da tabela com BeautifulSoup
                soup = BeautifulSoup(driver.page_source, "html.parser")
                table = soup.select_one(
                    "#card-fluxo-pagamento > article > article > section > div > div > table")

                rows = table.find_all("tr")
                # Make a dataframe WITH THE NAME OF THE ATIVO
                data_list = []
                for row in rows:
                    columns = row.find_all("td")
                    data = [col.text.strip() for col in columns]
                    if data:
                        print(data)
                        data_list.append(data)
                    print(ativos[i])

                df_append = pd.DataFrame(data_list)

                # Adiciona o nome do ativo em uma coluna no DataFrame
                df_append["Ativo"] = ativos[i]
                # Concat
                df = pd.concat([df, df_append])
            except Exception as e:
                print(f"Erro ao extrair taxa ANBIMA: {e}")

    # Definir os nomes das colunas
    columns = ["Dados do evento", "Data de pagamento",
               "Prazos (dias úteis)", "Dias entre pagamentos", "Expectativa de juros (%)", "Juros projetados", "Amortizações", "Fluxo descontado (R$)", "Ativo"]
    df.columns = columns

    # Salvar o DataFrame em um arquivo CSV
    df.to_csv("Dados/flux_deb.csv", index=False)
    print("Tabela salva com sucesso!")


except Exception as e:
    print(f"Ocorreu um erro: {e}")

finally:
    # Fechar o navegador
    driver.quit()
