import http from 'k6/http';

export function getSubject(subject){
    return http.get(`http://localhost:8081/subjects/${subject}/versions/1`)
}

