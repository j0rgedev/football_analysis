import uuid
from db.cassandra_connection import CassandraConnection
import webcolors

def rgb_to_name(rgb):
    try:
        return webcolors.rgb_to_name(tuple(map(int, rgb)))
    except ValueError:
        min_diff = float('inf')
        closest_color = None
        for name, value in webcolors.CSS3_NAMES_TO_HEX.items():
            r, g, b = webcolors.hex_to_rgb(value)
            diff = sum((c1 - c2) ** 2 for c1, c2 in zip(rgb, (r, g, b)))
            if diff < min_diff:
                min_diff = diff
                closest_color = name
        return closest_color


def guardar_datos(tracks, team_ball_control, video_id, keyspace="analitica_deportes"):
    cassandra = CassandraConnection(keyspace)
    cassandra.connect()

    query_jugadores_template = """
    INSERT INTO jugadores (id_jugador, id_video, numero_cuadro, equipo, color_equipo, posicion_x, posicion_y, velocidad, distancia_recorrida, tiene_balon)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    batch_size = 500
    batch = []

    for frame_num, players in enumerate(tracks['players']):
        for player_id, player_data in players.items():
            # Obtener posición
            posicion_x = float(player_data['position_transformed'][0]) if player_data['position_transformed'] else None
            posicion_y = float(player_data['position_transformed'][1]) if player_data['position_transformed'] else None

            # Determinar color del equipo
            team = str(player_data.get('team', '0'))
            team_color_vector = player_data.get('team_color', [])
            color_equipo = "N/A"

            try:
                if isinstance(team_color_vector, list) and len(team_color_vector) > int(team) - 1:
                    color_equipo = rgb_to_name(team_color_vector[int(team) - 1])
            except (ValueError, IndexError):
                color_equipo = "N/A"

            # Agregar consulta al lote
            batch.append((
                player_id,
                video_id,
                frame_num,
                team,
                color_equipo,
                posicion_x,
                posicion_y,
                player_data.get('speed', 0.0),
                player_data.get('distance', 0.0),
                player_data.get('has_ball', False)
            ))

            # Ejecutar el lote si alcanza el tamaño definido
            if len(batch) >= batch_size:
                cassandra.execute_batch(query_jugadores_template, batch)
                batch = []

    # Ejecutar cualquier resto del lote
    if batch:
        cassandra.execute_batch(query_jugadores_template, batch)

    # Insertar datos del balón
    query_balon = """
    INSERT INTO balon (id_balon, id_video, numero_cuadro, posicion_x, posicion_y, id_jugador_asignado, equipo_en_control)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    # Tamaño del batch
    batch_size = 500
    batch = []

    for frame_num, ball in enumerate(tracks['ball']):
        ball_bbox = ball.get(1, {}).get('bbox', [])
        assigned_player = ball.get(1, {}).get('assigned_player', None)
        equipo_en_control = team_ball_control[frame_num] if frame_num < len(team_ball_control) else None

        posicion_x = float(ball_bbox[0]) if ball_bbox else None
        posicion_y = float(ball_bbox[1]) if ball_bbox else None
        assigned_player = int(assigned_player) if assigned_player is not None else None
        equipo_en_control = str(equipo_en_control) if equipo_en_control is not None else None

        # Agregar datos al lote
        batch.append((
            uuid.uuid4(),
            video_id,
            frame_num,
            posicion_x,
            posicion_y,
            assigned_player,
            equipo_en_control
        ))

        # Ejecutar el lote si alcanza el tamaño definido
        if len(batch) >= batch_size:
            cassandra.execute_batch(query_balon, batch)
            batch = []  # Reiniciar el lote

    # Ejecutar cualquier resto del lote
    if batch:
        cassandra.execute_batch(query_balon, batch)


    # Cerrar conexión
    cassandra.close()
    print(f"Datos del video {video_id} guardados correctamente.")