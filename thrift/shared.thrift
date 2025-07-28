namespace go thrift.shared

struct SharedStruct {
    1: required i32    key  ,
    2: required string value,
}

service SharedService {
    SharedStruct getStruct(1: i32 key),
}