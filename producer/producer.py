import sys
import time
import cv2
import face_recognition
import numpy as np
import random         
from kafka import KafkaProducer

topic = "distributed-video1"

def publish_video(video_file):
    """
    Publish given video file to a specified Kafka topic.
    Kafka Server is expected to be running on the localhost. Not partitioned.
    
    :param video_file: path to video file <string>
    """
    # Start up producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    
    # Open file
    video = cv2.VideoCapture(video_file)
    
    print('publishing video...')

    while(video.isOpened()):
        success, frame = video.read()

        # Ensure file was read successfully
        if not success:
            print("bad read!")
            break
        
        # Convert image to png
        ret, buffer = cv2.imencode('.jpg', frame)

        # Convert to bytes and send to kafka
        producer.send(topic, buffer.tobytes())

        time.sleep(0.2)
    video.release()
    print('publish complete')

def publish_camera():
    """
    Publish camera video stream to specified Kafka topic.
    Kafka Server is expected to be running on the localhost. Not partitioned.
    """

    # Start up producer
    # producer = KafkaProducer(bootstrap_servers='localhost:9092')
    
    
    face_cascade = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')

    url = 'http://10.0.137.113:8080/video'
    video_capture = cv2.VideoCapture(url)
    # Load a second sample picture and learn how to recognize it.
    anh_nhan_image = face_recognition.load_image_file("/home/admin/Desktop/git/Kafka-detect-face/producer/Anh_Nhan.png")
    anh_nhan_face_encoding = face_recognition.face_encodings(anh_nhan_image)[0]

    # Load a second sample picture and learn how to recognize it.
    anh_gia_image = face_recognition.load_image_file("/home/admin/Desktop/git/Kafka-detect-face/producer/Anh_Gia.jpg")
    anh_gia_face_encoding = face_recognition.face_encodings(anh_gia_image)[0]

    # Load a second sample picture and learn how to recognize it.
    chi_hoa_image = face_recognition.load_image_file("/home/admin/Desktop/git/Kafka-detect-face/producer/Chi_Hoa.png")
    chi_hoa_face_encoding = face_recognition.face_encodings(chi_hoa_image)[0]

    known_face_encodings = [
    anh_nhan_face_encoding,
    chi_hoa_face_encoding,
    anh_gia_face_encoding,
    dat_face_encoding]

    known_face_names = [
        "Anh Nhan",
        "Chi Hoa",
        "Anh Gia",
    ]

# Initialize some variables
    face_locations = []
    face_encodings = []
    face_names = []
    process_this_frame = True
    # try:
    while(True):
        ret, frame = video_capture.read()

        small_frame = cv2.resize(frame, (0, 0), fx=0.25, fy=0.25)

        rgb_small_frame = small_frame[:, :, ::-1]

        if process_this_frame:
            face_locations = face_recognition.face_locations(rgb_small_frame)
            face_encodings = face_recognition.face_encodings(rgb_small_frame, face_locations)

            face_names = []
            for face_encoding in face_encodings:

                matches = face_recognition.compare_faces(known_face_encodings, face_encoding)
                name = "Chưa biết"

                face_distances = face_recognition.face_distance(known_face_encodings, face_encoding)
                best_match_index = np.argmin(face_distances)
                path_img = random.randrange(10000)
                if matches[best_match_index]:
                    name = known_face_names[best_match_index]
                else:
                    cv2.imwrite("lib/"+str(path_img)+"/1.jpg",frame)
                face_names.append(name)

        process_this_frame = not process_this_frame

        for (top, right, bottom, left), name in zip(face_locations, face_names):
            top *= 4
            right *= 4
            bottom *= 4
            left *= 4

            cv2.rectangle(frame, (left, top), (right, bottom), (0, 0, 255), 2)

            cv2.rectangle(frame, (left, bottom - 35), (right, bottom), (0, 0, 255), cv2.FILLED)
            font = cv2.FONT_HERSHEY_DUPLEX
            cv2.putText(frame, name, (left + 6, bottom - 6), font, 1.0, (255, 255, 255), 1)

        cv2.imshow('Video', frame)

        if cv2.waitKey(1) & 0xFF == ord('q'):
            break
            # # gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

            # # faces = face_cascade.detectMultiScale(gray, 1.1, 4)

            # # Draw the rectangle around each face
            # # font 
            # font = cv2.FONT_HERSHEY_SIMPLEX 
            
            # # org 
            # org = (50, 50) 
            
            # # fontScale 
            # fontScale = 1
            
            # # Blue color in BGR 
            # color = (255, 0, 0) 
            
            # # Line thickness of 2 px 
            # thickness = 2

            # for (x, y, w, h) in faces:
            #     cv2.rectangle(frame, (x, y), (x+w, y+h), (255, 0, 0), 2)
            #     cv2.putText(frame, str(len(faces)), org, font,fontScale, color, thickness, cv2.LINE_AA)
            # ret, buffer = cv2.imencode('.jpg', frame)
            # producer.send(topic, buffer.tobytes())
            
            # # Choppier stream, reduced load on processor
            # time.sleep(0.2)

        # except:
        #     print("\nExiting.")
        #     sys.exit(1)

    
    camera.release()


if __name__ == '__main__':
    """
    Producer will publish to Kafka Server a video file given as a system arg. 
    Otherwise it will default by streaming webcam feed.
    """
    if(len(sys.argv) > 1):
        video_path = sys.argv[1]
        publish_video(video_path)
    else:
        print("publishing feed!")
        publish_camera()