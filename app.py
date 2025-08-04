import logging
from flask import Flask, request, jsonify, render_template_string
import requests
from datetime import datetime
from collections import defaultdict

app = Flask(__name__)

# Token e URL da API externa
API_TOKEN = 'seu_token_aqui'
API_URL = 'https://hml-api.hackerexnova.com/api/deposit'

# Página HTML básica
HTML_PAGE = """
<!DOCTYPE html>
<html>
<head>
    <title>Ranking de Depósitos</title>
    <style>
        body { font-family: Arial; padding: 20px; background: #f5f5f5; }
        h1 { color: #333; }
        form, table { margin-top: 20px; }
        label { display: block; margin-top: 10px; }
        table { width: 100%; border-collapse: collapse; margin-top: 20px; background: white; }
        th, td { border: 1px solid #ccc; padding: 10px; text-align: left; }
        th { background: #eee; }
    </style>
</head>
<body>
    <h1>Ranking de Depósitos</h1>
    <form method="get" action="/ranking">
        <label>Data Início: <input type="date" name="start_date" required></label>
        <label>Data Fim: <input type="date" name="end_date" required></label>
        <label>Tipo de Ranking:
            <select name="type">
                <option value="daily">Diário</option>
                <option value="weekly">Semanal</option>
                <option value="monthly">Mensal</option>
            </select>
        </label>
        <button type="submit">Gerar Ranking</button>
    </form>
    {% if ranking %}
        <h2>Estatísticas Gerais</h2>
        <p>Total de usuários: {{ stats.total_users }}</p>
        <p>Total de depósitos: {{ stats.total_deposits }}</p>
        <p>Total depositado: R$ {{ stats.total_amount | round(2) }}</p>
        <p>Média por usuário: R$ {{ stats.average_per_user | round(2) }}</p>

        <h2>Ranking</h2>
        <table>
            <tr>
                <th>Posição</th>
                <th>Nome</th>
                <th>E-mail</th>
                <th>Total Depositado</th>
                <th>Nº de Depósitos</th>
                <th>Média</th>
            </tr>
            {% for idx, user in enumerate(ranking, 1) %}
            <tr>
                <td>{{ idx }}</td>
                <td>{{ user.name }}</td>
                <td>{{ user.email }}</td>
                <td>R$ {{ user.total_amount | round(2) }}</td>
                <td>{{ user.total_deposits }}</td>
                <td>R$ {{ user.average_deposit | round(2) }}</td>
            </tr>
            {% endfor %}
        </table>
    {% endif %}
</body>
</html>
"""

@app.route('/')
def index():
    return render_template_string(HTML_PAGE)

@app.route('/ranking', methods=['GET'])
def ranking():
    try:
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        ranking_type = request.args.get("type", "daily").lower()

        if not start_date or not end_date:
            return jsonify({"error": "Informe data de início e fim para gerar o ranking."}), 400

        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        headers = {"api-token": API_TOKEN}
        params = {
            "page": 1,
            "pageSize": 1000,
            "startDate": start_date,
            "endDate": end_date,
            "status": "APPROVED"
        }

        all_deposits = []
        current_page = 1
        has_more = True

        while has_more:
            params["page"] = current_page
            response = requests.get(API_URL, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            deposits = data.get("data", [])
            total_count = data.get("count", 0)
            all_deposits.extend(deposits)

            if len(all_deposits) >= total_count:
                has_more = False
            else:
                current_page += 1

        # Agrupar por usuário
        ranking_data = {}
        for dep in all_deposits:
            user = dep.get("user", {})
            user_id = user.get("id")
            if not user_id:
                continue

            name = user.get("name", "Desconhecido")
            email = user.get("email", "")
            amount = dep.get("amount", 0.0)
            created_at = dep.get("createdAt")

            try:
                dt = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                dt = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")

            if ranking_type == "weekly":
                period_key = dt.strftime("%Y-%W")
            elif ranking_type == "monthly":
                period_key = dt.strftime("%Y-%m")
            else:
                period_key = dt.strftime("%Y-%m-%d")

            if user_id not in ranking_data:
                ranking_data[user_id] = {
                    "id": user_id,
                    "name": name,
                    "email": email,
                    "total_amount": 0,
                    "total_deposits": 0,
                    "deposits_by_day": defaultdict(float)
                }

            ranking_data[user_id]["total_amount"] += amount
            ranking_data[user_id]["total_deposits"] += 1
            ranking_data[user_id]["deposits_by_day"][period_key] += amount

        # Processar lista
        ranking_list = list(ranking_data.values())
        for item in ranking_list:
            item["average_deposit"] = (
                item["total_amount"] / item["total_deposits"]
                if item["total_deposits"] > 0 else 0
            )

        ranking_list.sort(key=lambda x: x["total_amount"], reverse=True)

        stats = {
            "total_amount": sum(u["total_amount"] for u in ranking_list),
            "total_deposits": sum(u["total_deposits"] for u in ranking_list),
            "total_users": len(ranking_list),
            "average_per_user": (
                sum(u["total_amount"] for u in ranking_list) / len(ranking_list)
                if ranking_list else 0
            )
        }

        return render_template_string(HTML_PAGE, ranking=ranking_list, stats=stats)

    except Exception as e:
        logging.exception("Erro ao processar ranking")
        return jsonify({"error": "Erro ao gerar ranking", "detalhes": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
