## RUST DOCS
- https://wikidocs.net/book/16747

### main 함수
- Rust에서는 `fn main()`이 메인 함수임
- 파이썬에서는 `if __name__ == "__main__":`
```rust
fn main() {
  println!("hello");
}
```

### Path 접근 연산자
- `::` 파이썬의 `.`와 비슷
- String.valueOf("hello") ≈ String::from("hello")

### 자료 구조
```rust
// 배열 (Array)
let arr = [1,2,3,4,5];
let arr: [i32; 5] = [1, 2, 3, 4, 5];
arr.iter().map(|&x| x * 2);

println!("{}", arr[0]);


// Vector (리스트) -> 같은 타입만 가능
let mut nums = Vec::new();
let mut nums = vec![1,2,3]; // 이렇게 선언
let mut nums: Vec<i32> = vec![1,2,3];
nums.push(1);
nums.push(2);
nums.push(3);

// Tuple
- 여러 타입을 묶을 수 있음
let user = ("kim", 20, true);
let user: (&str, i32, bool) = ("kim", 20, true);

println!("{}", user.0);

// Struct (구조체)
// 파이썬의 클래스와 비슷, 속성만 있는 형태
// method는 trait를 사용해서 구현해야함
struct User {
    name: String,
    age: u32,
}

let user = User {
    name: String::from("kim"),
    age: 20,
};

println!("{}", user.name);
println!("{}", user.age);

//변경하려면 mut 필요:
user.age = 21;

// Enum
// 여러 가능한 상태중 하나를 표현
enum Direction {
    UP,DOWN,LEFT,RIGHT,
}

let dir = Direction::UP;

// match와 많이 쓰임
match dir {
    Direction::Up => println!("위"),
    Direction::Down => println!("아래"),
    Direction::Left => println!("왼쪽"),
    Direction::Right => println!("오른쪽"),
}

// HashMap
// 키-값 저장소, 딕셔너리와 동일
use std::collections::HashMap;

let mut map = HashMap::new();
let mut map: HashMap<String, i32> = HashMap::new();
map.insert("BTC", 75000);
map.insert("ETH", 3000);

match map.get("BTC") {
    Some(value) => println!("값 있음: {}", value),
    None => println!("값 없음"),
}


// option
// 값이 있을 수도 있고 없을 수도 있는 자료구조
let a: Option<i32> = Some(10);
let b: Option<i32> = None;

match a {
    Some(value) => println!("값 있음: {}", value),
    None => println!("값 없음"),
}

// Result
// 성공 또는 실패를 표현
// 성공하면 특정타입값, 실패하면 값 또는 에러 지정.
// HTTP 요청, 파일 읽기, JSON 파싱처럼 실패할 수 있는 작업은 대부분 Result를 씁
let result: Result<i32, String> = Ok(10);
let error: Result<i32, String> = Err(String::from("실패"));
match result {
    Ok(value) => println!("성공: {}", value),
    Err(err) => println!("실패: {}", err),
}


//Tuple Struct
// 이름있는 튜플 느낌
struct Point(i32, i32);
let p = Point(10,20);
println!("{}", p.0);
println!("{}", p.1);
```

### 문자열: String / &str
**String**
- 소유권이 있는 문자열
- 변경가능
```rust
let s = String::from("hello");
s.push_str(", world!");
```

**&str**
- 문자열을 직접 소유하지 않고 빌려서 사용하는 것
- 변경불가
```rust
let s: &str = "hello";
let s = String::from("hello");
let borrowed = &s;

**소유권 규칙 (Ownership Rules)**

1. 소유권은 단 하나의 변수만 가질 수 있다.
2. 소유권은 스코프(Scope)를 벗어나면 자동으로 해제된다.
3. 빌림(Borrowing): 소유권을 빌려줄 때는 & (불변 참조), &mut (가변 참조)를 사용한다.
4. 빌림의 규칙: 불변 참조는 여러 개가 가능하지만, 가변 참조는 단 하나만 가능하고, 가변 참조가 있는 동안에는 불변 참조를 사용할 수 없다.
```

### 라이프타임
- 참조는 원본보다 오래 살아 있으면 안 됨.
- 'a는 그 빌린 문자열이 얼마나 오래 살아 있어야 하는지를 표시하는 라이프타임
```
let s = String::from("hello"); -> 
let r = &s;

// s ───────> "hello"
// ^
// |
// r
``` 

### <'a>랑 &'a str
- url: &'a str: 이 구조체는 문자열을 직접 소유하지 않고, 어딘가에 있는 문자열을 잠깐 빌려서 사용
- 'a는 그 빌린 문자열이 얼마나 오래 살아 있어야 하는지를 표시하는 라이프타임 -> 어떤 라이프타임에 붙인 이름
- 'a는 “a초”, “a분” 이런 게 아니라, 컴파일러에게 알려주는 범위 이름표



### println! 매크로 포맷 지정자
- {} : 기본
- {:?} : Debug 트레이트
- {:#?} : pretty Debug 트레이트
- {:x} : 16진수

```rust
#[derive(Debug)]
struct Point {
    x: i32,
    y: i32,
}

fn main() {
    let p = Point { x: 1, y: 2 };
    println!("{:?}", p);
}
//
```

### use 문법
- Python의 import

### 속성(attribute) 문법
- 컴파일러나 매크로에게 주는 지시문
- 약간 데코레이터 같은 것,


```rust
#[derive(Serialize)]
struct ProxyPayload<'a> {
    url: &'a str,
}
```


### Trait란?
- 어떤 타입이 반드시 가져야 하는 기능의 약속
- Java의 interface와 비슷
  - Java interface      ≈ Rust `trait`
  - Java implements     ≈ Rust `impl Trait for Type`
- Rust의 기본 타입들은 많은 trait를 가지고 있음. 예를 들어 Debug, Clone, Copy 등등
```rust
trait Speak {
    fn speak(&self);
}

struct Dog;
struct Cat;

impl Speak for Dog {
    fn speak(&self) {
        println!("멍멍");
    }
}

impl Speak for Cat {
    fn speak(&self) {
        println!("야옹");
    }
}
```

### Derive
- 특정 trait 구현을 자동으로 만들어주는 기능
```rust
#[derive(Serialize)]
struct ProxyPayload<'a> {
    url: &'a str,
}

위 코드와 같은 기능은 

impl Serialize for ProxyPayload {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_struct("ProxyPayload", 1)?;
        seq.serialize_field("url", self.url)?;
        seq.end()
    }
}
```