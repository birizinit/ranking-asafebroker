from flask import Flask, render_template, request, jsonify
import requests
import os
import logging
from datetime import datetime, timedelta
from collections import defaultdict

app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_URL = os.environ.get("API_URL", "https://api.asafebroker.com/admin-token/deposits")
API_TOKEN = os.environ.get("API_TOKEN", "o7efkbcw58")

@app.route("/")
def index():
    logging.info("Servindo ranking de depósitos")
    return render_template("index.html")

@app.route("/health")
def health():
    return {"status": "healthy", "message": "Ranking app is running"}

@app.route("/data")
def data():
    try:
        page = int(request.args.get("page", 1))
        page_size = int(request.args.get("pageSize", 25))
        is_influencer = request.args.get("isInfluencer", "false").lower() == "true"
        start_date = request.args.get("startDate")
        end_date = request.args.get("endDate")
        order_by = request.args.get("orderBy", "amount")
        order_direction = request.args.get("orderDirection", "DESC")
        status = request.args.get("status", "APPROVED")
        search = request.args.get("search", "")
    except ValueError as e:
        logging.error(f"Erro de validação de parâmetro: {e}")
        return jsonify({"error": "Parâmetros de requisição inválidos", "details": str(e)}), 400

    params = {
        "page": page,
        "pageSize": page_size,
        "isInfluencer": str(is_influencer).lower(),
        "startDate": start_date,
        "endDate": end_date,
        "orderBy": order_by,
        "orderDirection": order_direction,
        "status": status,
    }

    logging.info(f"Requisição recebida para /data com parâmetros: {params}")

    try:
        headers = {"api-token": API_TOKEN}
        logging.info(f"Fazendo requisição para API externa: {API_URL} com params: {params}")
        response = requests.get(API_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # Aplicar filtro de pesquisa local se fornecido
        if search and data.get("data"):
            filtered_data = []
            search_lower = search.lower()
            for item in data["data"]:
                user = item.get("user", {})
                if (search_lower in user.get("name", "").lower() or 
                    search_lower in user.get("email", "").lower() or
                    search_lower in item.get("method", "").lower() or
                    search_lower in item.get("provider", "").lower()):
                    filtered_data.append(item)
            data["data"] = filtered_data
            data["count"] = len(filtered_data)
        
        logging.info("Dados da API externa recebidos com sucesso.")
        return jsonify(data)
    except requests.exceptions.Timeout:
        logging.error("Timeout ao conectar com a API externa.")
        return jsonify({"error": "A API externa demorou muito para responder."}), 504
    except requests.exceptions.ConnectionError as e:
        logging.error(f"Erro de conexão com a API externa: {e}")
        return jsonify({"error": "Não foi possível conectar à API externa."}), 503
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao buscar dados da API externa: {e}, Resposta: {response.text if 'response' in locals() else 'N/A'}")
        return jsonify({"error": "Erro ao buscar dados da API externa", "details": str(e)}), 500
    except Exception as e:
        logging.critical(f"Erro inesperado no endpoint /data: {e}")
        return jsonify({"error": "Ocorreu um erro inesperado no servidor."}), 500

@app.route("/user-balances")
def user_balances():
    try:
        page = int(request.args.get("page", 1))
        page_size = int(request.args.get("pageSize", 25))
        order_by = request.args.get("orderBy", "user.balance")
        order_direction = request.args.get("orderDirection", "DESC")
        search = request.args.get("search", "")
    except ValueError as e:
        logging.error(f"Erro de validação de parâmetro: {e}")
        return jsonify({"error": "Parâmetros de requisição inválidos", "details": str(e)}), 400

    logging.info(f"Requisição recebida para /user-balances com page={page}, pageSize={page_size}, orderBy={order_by}, orderDirection={order_direction}")

    all_users_with_balances = {}
    current_api_page = 1
    external_api_page_size = 100
    has_more_data = True
    total_deposits_fetched = 0

    try:
        headers = {"api-token": API_TOKEN}
        while has_more_data:
            params = {
                "page": current_api_page,
                "pageSize": external_api_page_size,
                "status": "APPROVED",
                "orderBy": "createdAt",
                "orderDirection": "DESC"
            }
            
            logging.info(f"Fazendo requisição para API externa de depósitos para coletar saldos: {API_URL} com params: {params}")
            response = requests.get(API_URL, headers=headers, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()
            deposits = data.get("data", [])
            total_deposits_from_api = data.get("count", 0)
            
            if not deposits:
                has_more_data = False
                break

            total_deposits_fetched += len(deposits)
            
            for deposit in deposits:
                user_info = deposit.get("user")
                if user_info and user_info.get("id"):
                    user_id = user_info["id"]
                    
                    real_balance = None
                    if user_info.get("wallets"):
                        for wallet in user_info["wallets"]:
                            if wallet.get("type") == "REAL":
                                real_balance = wallet.get("balance")
                                break

                    if user_id not in all_users_with_balances:
                        all_users_with_balances[user_id] = {
                            "id": user_id,
                            "name": user_info.get("name"),
                            "email": user_info.get("email"),
                            "nickname": user_info.get("nickname"),
                            "phone": user_info.get("phone"),
                            "country": user_info.get("country"),
                            "lastLoginAt": user_info.get("lastLoginAt"),
                            "user.balance": real_balance
                        }
                    elif all_users_with_balances[user_id].get("user.balance") is None and real_balance is not None:
                        all_users_with_balances[user_id]["user.balance"] = real_balance

            current_api_page += 1
            
            if total_deposits_fetched >= total_deposits_from_api and total_deposits_from_api > 0:
                has_more_data = False
            if current_api_page > 50:
                logging.warning("Limite de 50 páginas da API externa atingido para coletar saldos de usuários.")
                has_more_data = False

    except requests.exceptions.Timeout:
        logging.error("Timeout ao conectar com a API externa para coletar saldos.")
        return jsonify({"error": "A API externa demorou muito para responder ao coletar saldos."}), 504
    except requests.exceptions.ConnectionError as e:
        logging.error(f"Erro de conexão com a API externa ao coletar saldos: {e}")
        return jsonify({"error": "Não foi possível conectar à API externa para coletar saldos."}), 503
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao buscar dados da API externa para coletar saldos: {e}")
        return jsonify({"error": "Erro ao buscar dados da API externa para coletar saldos", "details": str(e)}), 500
    except Exception as e:
        logging.critical(f"Erro inesperado no endpoint /user-balances: {e}")
        return jsonify({"error": "Ocorreu um erro inesperado no servidor ao coletar saldos."}), 500

    users_list = list(all_users_with_balances.values())
    
    # Aplicar filtro de pesquisa
    if search:
        search_lower = search.lower()
        users_list = [user for user in users_list 
                     if search_lower in user.get("name", "").lower() or 
                        search_lower in user.get("email", "").lower() or
                        search_lower in user.get("nickname", "").lower()]

    # Ordenar os usuários
    if order_by == "user.balance":
        users_list.sort(key=lambda x: x.get("user.balance") if x.get("user.balance") is not None else (-float('inf') if order_direction == "ASC" else float('inf')),
                       reverse=(order_direction == "DESC"))
    elif order_by == "name":
        users_list.sort(key=lambda x: x.get("name", "").lower(), reverse=(order_direction == "DESC"))
    elif order_by == "lastLoginAt":
        users_list.sort(key=lambda x: x.get("lastLoginAt", ""), reverse=(order_direction == "DESC"))

    total_users = len(users_list)
    
    # Aplicar paginação
    start_index = (page - 1) * page_size
    end_index = start_index + page_size
    paginated_users = users_list[start_index:end_index]

    response_data = {
        "data": paginated_users,
        "currentPage": page,
        "lastPage": (total_users + page_size - 1) // page_size,
        "count": total_users
    }

    logging.info(f"Retornando {len(paginated_users)} usuários paginados com saldos.")
    return jsonify(response_data)

@app.route("/ranking")
def ranking():
    try:
        start_date = request.args.get("startDate")
        end_date = request.args.get("endDate")
        ranking_type = request.args.get("type", "daily")  # daily, weekly, monthly
        
        if not start_date or not end_date:
            return jsonify({"error": "Datas de início e fim são obrigatórias"}), 400
            
    except ValueError as e:
        logging.error(f"Erro de validação de parâmetro: {e}")
        return jsonify({"error": "Parâmetros de requisição inválidos", "details": str(e)}), 400

    # Buscar todos os depósitos do período
    all_deposits = []
    current_page = 1
    has_more_data = True
    
    try:
        headers = {"api-token": API_TOKEN}
        
        while has_more_data:
            params = {
                "page": current_page,
                "pageSize": 100,
                "startDate": start_date,
                "endDate": end_date,
                "status": "APPROVED",
                "orderBy": "createdAt",
                "orderDirection": "DESC"
            }
            
            logging.info(f"Buscando página {current_page} da API")
            response = requests.get(API_URL, headers=headers, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()
            
            deposits = data.get("data", [])
            if not deposits:
                has_more_data = False
                break
                
            all_deposits.extend(deposits)
            current_page += 1
            
            # Limite de segurança
            if current_page > 50:
                logging.warning("Limite de 50 páginas atingido")
                has_more_data = False
                
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao buscar dados da API: {e}")
        return jsonify({"error": "Erro ao buscar dados da API", "details": str(e)}), 500
    except Exception as e:
        logging.critical(f"Erro inesperado: {e}")
        return jsonify({"error": "Erro inesperado no servidor"}), 500

    # Processar dados para ranking
    user_totals = defaultdict(lambda: {
        'total_amount': 0,
        'total_deposits': 0,
        'user_info': None,
        'deposits_by_day': defaultdict(float)
    })
    
    for deposit in all_deposits:
        user_info = deposit.get("user", {})
        user_id = user_info.get("id")
        amount = deposit.get("amount", 0)
        
        if user_id and amount:
            user_totals[user_id]['total_amount'] += amount
            user_totals[user_id]['total_deposits'] += 1
            user_totals[user_id]['user_info'] = user_info
            
            # Agrupar por dia para gráfico
            if deposit.get("approvedAt"):
                day = datetime.fromisoformat(deposit["approvedAt"].replace('Z', '+00:00')).strftime('%Y-%m-%d')
                user_totals[user_id]['deposits_by_day'][day] += amount
    
    # Criar ranking
    ranking_data = []
    for user_id, data in user_totals.items():
        user_info = data['user_info']
        if user_info:
            ranking_data.append({
                'user_id': user_id,
                'name': user_info.get('name', 'N/A'),
                'email': user_info.get('email', 'N/A'),
                'total_amount': data['total_amount'],
                'total_deposits': data['total_deposits'],
                'average_deposit': data['total_amount'] / data['total_deposits'] if data['total_deposits'] > 0 else 0,
                'deposits_by_day': dict(data['deposits_by_day'])
            })
    
    # Ordenar por valor total
    ranking_data.sort(key=lambda x: x['total_amount'], reverse=True)
    
    # Adicionar posições
    for i, user in enumerate(ranking_data):
        user['position'] = i + 1
    
    # Estatísticas gerais
    total_amount = sum(user['total_amount'] for user in ranking_data)
    total_deposits = sum(user['total_deposits'] for user in ranking_data)
    total_users = len(ranking_data)
    
    stats = {
        'total_amount': total_amount,
        'total_deposits': total_deposits,
        'total_users': total_users,
        'average_per_user': total_amount / total_users if total_users > 0 else 0,
        'period_start': start_date,
        'period_end': end_date
    }
    
    logging.info(f"Ranking gerado com {len(ranking_data)} usuários")
    
    return jsonify({
        'ranking': ranking_data[:50],  # Top 50
        'stats': stats,
        'success': True
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
