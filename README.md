# 🚀 Pipeline ETL - Integração de Dados Empresariais

Um sistema de **Extract, Transform, Load (ETL)** desenvolvido em Python para migração e transformação de dados entre diferentes sistemas de banco de dados, com foco em flexibilidade, escalabilidade e monitoramento.

## 📋 Sobre o Projeto

Este projeto foi desenvolvido para automatizar o processo de migração de dados entre diferentes sistemas empresariais, permitindo extrair dados de bancos Firebird, SQL Server (outros futuramente), aplicar transformações personalizadas e carregar os dados limpos no destino final.

### 🎯 Principais Características

- **Arquitetura Modular**: Implementação baseada em interfaces e padrões de design
- **Suporte Multi-Database**: Firebird e SQL Server
- **Transformações Dinâmicas**: Sistema flexível de transformação de dados
- **Monitoramento em Tempo Real**: Progress bars e logging detalhado
- **Configuração Externa**: Configuração via arquivos JSON
- **Tratamento de Erros**: Sistema de captura e logging de erros

## 🏗️ Arquitetura do Sistema

O projeto segue uma arquitetura limpa e modular, conforme demonstrado no diagrama UML abaixo:

![Diagrama UML do Sistema](/src/docs/diagramaUML.png)

### Componentes Principais:

- **MainPipeline**: Orquestrador principal do fluxo ETL
- **SQLConnector**: Gerenciador de conexões com banco de dados
- **Extractor**: Responsável pela extração de dados
- **Transformer**: Engine de transformação com múltiplas operações
- **Loader**: Carregador de dados no destino final
- **FieldHandler**: Biblioteca de transformações de campos
- **JsonConfig**: Gerenciador de configurações
- **Log**: Sistema centralizado de logging

## 🛠️ Tecnologias Utilizadas

- **Python 3.12.10+**
- **SQLAlchemy**: ORM e gerenciamento de conexões
- **Pandas**: Manipulação e análise de dados
- **Pytest**: Controle de testes
- **Rich**: Interface de linha de comando melhorada
- **FDB**: Driver para Firebird
- **PyODBC**: Driver para SQL Server

## 📁 Estrutura do Projeto

```
projeto-etl/
    ├── src/
    │    │
    │    ├── docs/
    │    │   └── diagramaUML.py
    │    │
    │    ├── stages/
    │    │   ├── contracts/                     # Contratos de dados
    │    │   │   ├── extract_contract.py
    │    │   │   └── transform_contract.py
    │    │   │
    │    │   ├── extract/                       # Lógica de extração
    │    │   │   └── sql_extractor.py
    │    │   │
    │    │   ├── interfaces/                    # Implementações
    │    │   │   ├── load_data.py
    │    │   │   ├── sql_extractor.py
    │    │   │   └── transform_data.py
    │    │   │
    │    │   ├── load/                          # Lógica de carga
    │    │   │   └── load_data.py
    │    │   │
    │    │   └── transform/                     # Lógica de transformação
    │    │       ├── field_utils.py             # Transformações de campos
    │    │       └── transform_data.py
    │    │
    │    ├── tests/
    │    │   ├── field_utils_test.py
    │    │   ├── firebird_connector_test.py
    │    │   ├── log_test.py
    │    │   ├── main_pipeline_test.py
    │    │   └── sqlserver_connector_test.py
    │    │
    │    ├── utils/
    │    │   ├── citys.json                     # Base de cidades
    │    │   ├── config_json.py                 # Leitor de configurações
    │    │   └── connector.py                   # Gerenciador de conexões
    │    │   ├── destiny.json                   # Configuração do banco destino
    │    │   ├── log.py                         # Sistema de logging
    │    │   ├── origin.json                    # Configuração do banco origem
    │    │   └── systems.json                   # Mapeamento de tabelas
    │    │
    │    └── main_pipeline.py                   # Ponto de entrada da aplicação
    │    
    ├── .gitignore
    ├── .pylintrc
    ├── README.md
    └── requirements.txt
```

## ⚙️ Configuração

### 1. Configuração do Banco de Origem (origin.json)
```json
{
  "font": "Firebird",
  "host": "localhost",
  "user": "sysdba", 
  "password": "senha",
  "database": "caminho/para/database.fdb"
}
```

### 2. Configuração do Banco de Destino (destiny.json)
```json
{
  "font": "SQLServer",
  "host": "localhost",
  "user": "usuario", 
  "password": "senha",
  "database": "BaseDeDados",
  "driver": "driver=ODBC+Driver+17+for+SQL+Server"
}
```

### 3. Mapeamento de Tabelas (systems.json)
```json
{
    "supplier": {
        "table": "pessoas_origem",
        "destiny": "pessoas_destino",
        "fields": {
            "codigo_origem": {
                "field_destiny": "codigo_destino",
                "transform": {
                }
            },
            "nome_origem": {
                "field_destiny": "nome_destino",
                "transform": {
                    "trim": true,
                    "clear": true
                }
            },
            "tipo_origem": {
                "field_destiny": "tipo_destino",
                "transform": {
                    "select": "FORNECEDOR",
                    "switch": {
                        "str_from": ["FORNECEDOR"],
                        "str_to": ["J"]
                    }
                }
            },
            "cnpj_origem": {
                "field_destiny": "cnpj_destino",
                "transform": {
                    "format": "CNPJ"
                }
            }
        },
        "remove": {
            "column_1": "coluna_a",
            "column_2": "coluna_b",
            "column_3": "coluna_c"
        }
    }
}
```

## 🚀 Como Usar

### Instalação
```bash
# Clone o repositório
git clone https://github.com/seu-usuario/pipeline-etl.git

# Instale as dependências
pip3 install -r requirements.txt

# Configure os arquivos JSON em utils/
```

### Execução
```bash
python main_pipeline.py
```

## 🔧 Transformações Disponíveis

O sistema oferece diversas transformações para campos:

- **trim**: Remove espaços em branco das laterais
- **upper/lower**: Conversão de maiúsculas/minúsculas
- **switch**: Substituição de valores baseada em mapeamento de/para
- **format**: Formatação de CPF, CNPJ, CEP, datas
- **clear**: Limpeza e troca de caracteres especiais
- **split**: Divisão de campos (ex: telefone em DDD + número)
- **search**: Enriquecimento com dados externos (ex: cidades)
- **copy**: Cópia de valores entre campos
- **select**: Filtro de registros

## 📊 Monitoramento

O sistema fornece:
- Progress bars em tempo real durante execução
- Logs detalhados em `app.log`
- Arquivos Excel de cada tabela processada
- Informações de contexto em caso de erro

## 🔍 Exemplos de Execução

### Execução normal:
![Demonstração sem erro](/src/docs/execucao.gif)

### Execução com erro:
![Demonstração com erro](/src/docs/execucao_erro.gif)


## 🤝 Contribuições

Este projeto está aberto para contribuições! Sinta-se à vontade para:
- Reportar bugs
- Sugerir melhorias
- Adicionar novos drivers de banco
- Implementar novas transformações

## 📧 Contato

**Seu Nome**
- LinkedIn: [linkedin.com/in/victorhenriqs](https://www.linkedin.com/in/victorhenriqs/)
- Email: victorhenri.profess@gmail.com
- GitHub: [@victor-henri](https://github.com/victor-henri)

---

⭐ Se este projeto foi útil ou achou interessante, considere dar uma estrela no repositório!