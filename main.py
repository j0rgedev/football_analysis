import os
from db.dao import guardar_datos, verificar_existencia_y_limpiar
from utils import read_video, save_video
from trackers import Tracker
import time
import numpy as np
from team_assigner import TeamAssigner
from player_ball_assigner import PlayerBallAssigner
from camera_movement_estimator import CameraMovementEstimator
from view_transformer import ViewTransformer
from speed_and_distance_estimator import SpeedAndDistance_Estimator


def process_video(video_path, output_path, video_id):
    start_time = time.time()
    print(f"Processing video: {video_path}")

    # Read Video
    video_frames = read_video(video_path)
    print(f"Time to read video: {time.time() - start_time}")

    # Initialize Tracker
    start_time1 = time.time()
    tracker = Tracker('./models/best.pt')

    tracks = tracker.get_object_tracks(video_frames,
                                       read_from_stub=True,
                                       stub_path='./stubs/track_stubs.pkl')

    # Get object positions
    tracker.add_position_to_tracks(tracks)

    print(f"Time to track objects: {time.time() - start_time1}")

    # Camera movement estimator
    start_time2 = time.time()
    camera_movement_estimator = CameraMovementEstimator(video_frames[0])
    camera_movement_per_frame = camera_movement_estimator.get_camera_movement(video_frames,
                                                                               read_from_stub=True,
                                                                               stub_path='./stubs/camera_movement_stub.pkl')
    camera_movement_estimator.add_adjust_positions_to_tracks(tracks, camera_movement_per_frame)

    print(f"Time to estimate camera movement: {time.time() - start_time2}")

    # View Transformer
    start_time3 = time.time()
    view_transformer = ViewTransformer()
    view_transformer.add_transformed_position_to_tracks(tracks)
    print(f"Time to transform view: {time.time() - start_time3}")

    # Interpolate Ball Positions
    start_time4 = time.time()
    tracks["ball"] = tracker.interpolate_ball_positions(tracks["ball"])
    print(f"Time to interpolate ball positions: {time.time() - start_time4}")

    # Speed and Distance Estimator
    start_time5 = time.time()
    speed_and_distance_estimator = SpeedAndDistance_Estimator()
    speed_and_distance_estimator.add_speed_and_distance_to_tracks(tracks)
    print(f"Time to estimate speed and distance: {time.time() - start_time5}")

    # Assign Player Teams
    start_time6 = time.time()
    team_assigner = TeamAssigner()
    team_assigner.assign_team_color(video_frames[0],
                                    tracks['players'][0])

    for frame_num, player_track in enumerate(tracks['players']):
        if frame_num >= len(video_frames):
            break
        for player_id, track in player_track.items():
            team = team_assigner.get_player_team(video_frames[frame_num],
                                                 track['bbox'],
                                                 player_id)
            tracks['players'][frame_num][player_id]['team'] = team
            tracks['players'][frame_num][player_id]['team_color'] = team_assigner.team_colors[team]

    print(f"Time to assign player teams: {time.time() - start_time6}")

    # Assign Ball Acquisition
    start_time7 = time.time()
    player_assigner = PlayerBallAssigner()
    team_ball_control = []
    for frame_num, player_track in enumerate(tracks['players']):
        ball_bbox = tracks['ball'][frame_num][1]['bbox']
        assigned_player = player_assigner.assign_ball_to_player(player_track, ball_bbox)

        if assigned_player != -1:
            if 'team' in tracks['players'][frame_num][assigned_player]:
                tracks['players'][frame_num][assigned_player]['has_ball'] = True
                team_ball_control.append(tracks['players'][frame_num][assigned_player]['team'])
            else:
                team_ball_control.append(team_ball_control[-1])
        else:
            team_ball_control.append(team_ball_control[-1])
    team_ball_control = np.array(team_ball_control)
    print(f"Time to assign ball acquisition: {time.time() - start_time7}")

    # Draw output
    start_time8 = time.time()
    output_video_frames = tracker.draw_annotations(video_frames, tracks, team_ball_control)
    print(f"Time to draw object tracks: {time.time() - start_time8}")

    # Save video
    print("Saving video...")
    save_video(output_video_frames, output_path)
    print(f"Video saved to {output_path}")

    # Save to Cassandra
    print("Saving to Cassandra...")
    guardar_datos(tracks, team_ball_control, video_id=video_id)
    print("Saved to Cassandra!")


def main():
    input_folder = './input_videos'
    output_folder = './output_videos'

    os.makedirs(output_folder, exist_ok=True)

    # Preguntar al usuario si desea procesar todos los videos o uno específico
    print("¿Deseas procesar todos los videos o solo un video específico?")
    print("1. Procesar todos los videos")
    print("2. Procesar un video específico")
    choice = input("Elige una opción (1/2): ")

    if choice == "1":
        # Procesar todos los videos
        for video_file in os.listdir(input_folder):
            if video_file.endswith(('.mp4', '.avi', '.mov')):  # Filtrar por extensiones válidas
                video_path = os.path.join(input_folder, video_file)
                output_path = os.path.join(output_folder, f'output_{os.path.splitext(video_file)[0]}.mp4')

                # Usar el nombre del archivo como ID del video
                video_id = os.path.splitext(video_file)[0]
                print(f"Verificando existencia del video {video_file}...")

                # Verificar existencia y limpiar si es necesario
                status = verificar_existencia_y_limpiar(video_id)

                # Procesar el video solo si tiene las keys válidas
                if status in ["exists_in_jugadores", "exists_in_balon", "does_not_exist"]:
                    print(f"Procesando video {video_id} con estado: {status}...")
                    process_video(video_path, output_path, video_id)
                    print(f"Procesamiento de video {video_file} completado.")
            else:
                print(f"El archivo {video_file} no es un video compatible, se omitirá.")

    elif choice == "2":
        # Procesar un solo video
        video_name = input("Ingresa el nombre del archivo de video (incluye la extensión, e.g., video.mp4): ")
        video_path = os.path.join(input_folder, video_name)

        if os.path.isfile(video_path) and video_name.endswith(('.mp4', '.avi', '.mov')):
            output_path = os.path.join(output_folder, f'output_{os.path.splitext(video_name)[0]}.mp4')

            # Usar el nombre del archivo como ID del video
            video_id = os.path.splitext(video_name)[0]
            print(f"Verificando existencia del video {video_name}...")

            # Verificar existencia y limpiar si es necesario
            status = verificar_existencia_y_limpiar(video_id)

            # Procesar el video solo si tiene las keys válidas
            if status in ["exists_in_jugadores", "exists_in_balon", "does_not_exist"]:
                print(f"Procesando video {video_id} con estado: {status}...")
                process_video(video_path, output_path, video_id)
                print(f"Procesamiento de video {video_name} completado.")
        else:
            print(f"El archivo {video_name} no existe en la carpeta {input_folder} o no es un formato compatible.")

    else:
        print("Opción no válida. El programa se cerrará.")
        return

if __name__ == '__main__':
    main()