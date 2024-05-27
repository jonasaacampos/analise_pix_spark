


## Análise de Transações PIX

### Entendimento do Negócio

Através da base de transações do pix o banco deseja entender qual é o perfil dos clientes que utilizam o pix, além de verificar possíveis transações que tenham fraude. Porém, eles tem um cliente específico que tem um relacionamento muito bom para o banco, por isso, você recebeu a base de transações de cliente dos últimos 2 anos e precisa a partir dela criar um relatório contendo as principais características das transações. 

### Objetivos

- Limpar e pré-processar os dados das transações PIX
- Analisar padrões de uso do PIX, tais como os canais mais utilizados e os valores de transação mais comuns
- Use o PySpark MLlib para treinar e avaliar um modelo de detecção de fraude
- Avaliar o desempenho do modelo e fazer recomendações para melhorias futuras

### Metodologia

Para esta análise usarei a metodologia CRISP-DM. Fiz algumas anotações sobre esta metodologia e compartilhei neste repositório https://github.com/jonasaacampos/CRISP-DM

Há um pdf com os principais passos que utilizo no CRSIP-DM que compartilhei no linkedin, e pode ser acessado [neste link](https://www.linkedin.com/posts/jonasaacampos_crisp-dm-guia-para-consulta-r%C3%A1pida-activity-7186406004991877120-4Uun?utm_source=share&utm_medium=member_desktop)


### Dados

O conjunto de dados inclui as seguintes informações para cada transação:
- Detalhes da transação: valor, tempo, remetente e receptor CPF/CNPJ, tipo
- Etiqueta de fraude: uma variável binária que indica se a transação foi fraudulenta (1) ou não (0)

<details>
<summary>Detalhes do arquivo json (estrutura dos dados)</summary>

#### amostra do arquivo

```json
 {
        "id_transacao": 100998,
        "valor": 4339.33,
        "remetente": {
            "nome": "Jonathan Gonsalves",
            "banco": "BTG",
            "tipo": "PF"
        },
        "destinatario": {
            "nome": "Alana Castro",
            "banco": "Caixa",
            "tipo": "PJ"
        },
        "chave_pix": "cpf",
        "categoria": "transferencia",
        "transaction_date": "2022-09-25 09:50:35",
        "fraude": 0
    }

```

```json
{
  "id_transacao": inteiro,
  "valor": texto,
  "remetente": {
      "nome": texto,
      "banco": texto,
      "tipo": texto
  }, 
  "destinatario": {
      "nome": texto, 
      "banco":texto,
      "tipo": texto
  },        
  "categoria": texto,
  "transaction_date":texto,
  "chave_pix":texto,
  "fraude":inteiro,
}
```

</details>

### Tarefas ✅

- [ ] Normalização dos dados
  - [ ] O dataset será em  `*.json`. Analisar a estrutura do arquivo
  - [ ] Faça sua transformação para formato colunar
- [ ] Análise Exploratória de Dados: Use o PySpark para analisar padrões de uso do PIX
  - [ ] chaves pix mais usadas;
  - [ ] os valores de transação mais comuns;
  - [ ] distribuição dos valores de transação
    - [ ] hora
    - [ ] dia
  - [ ] quais bancos receberam mais transferências por dia;
  - [ ] para qual tipo de pessoa (PF ou PJ) foram realizadas mais transações
- [ ] Engenharia de Recursos: 
  - [ ] Apresentar novas características que podem ser úteis para a detecção de fraudes, tais como o número de transações feitas pelo mesmo remetente em um período de tempo específico.
- [ ] Modelagem: Use o PySpark MLlib para treinar e detectar possíveis transações que contenham fraude.


