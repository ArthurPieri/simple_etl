db = db.getSiblingDB('mydatabase');

users = [
    {
        "_id": ObjectId("507f1f77bcf86cd799439011"),
        "nome": "João",
        "idade": 28,
        "isVerified": true,
        "hobbies": ["nadar", "correr"],
        "endereço": {
            "rua": "Rua A",
            "numero": 123
        },
        "aniversario": new Date("1995-01-01T00:00:00Z"),
        "campoNulo": null,
        "fotoPerfil": new BinData(0, "binaryDataHere"),
        "website": "https://www.example.com/",
        "funcaoJS": function() { print("Hello!"); },
        "tag": "dev"
    },
    {
        "_id": ObjectId("507f1f77bcf86cd799439012"),
        "nome": "Maria",
        "idade": 24,
        "isVerified": false,
        "hobbies": ["ler", "escrever"],
        "endereço": {
            "rua": "Rua B",
            "numero": 456
        },
        "aniversario": new Date("1999-05-05T00:00:00Z"),
        "campoNulo": null,
        "fotoPerfil": new BinData(0, "binaryDataHere2"),
        "website": "https://www.maria.com/",
        "funcaoJS": function() { print("Hi!"); },
        "tag": "writer"
    },
    {
        "_id": ObjectId("507f1f77bcf86cd799439013"),
        "nome": "Pedro",
        "idade": 30,
        "isVerified": true,
        "hobbies": ["pescar", "acampar"],
        "endereço": {
            "rua": "Rua C",
            "numero": 789
        },
        "aniversario": new Date("1993-03-03T00:00:00Z"),
        "campoNulo": null,
        "fotoPerfil": new BinData(0, "binaryDataHere3"),
        "website": "https://www.pedro.com/",
        "funcaoJS": function() { print("Hey!"); },
        "tag": "adventurer"
    },
    {
        "_id": ObjectId("507f1f77bcf86cd799439014"),
        "nome": "Lucas",
        "idade": 32,
        "isVerified": false,
        "historicoPedidos": [
            {
                "idPedido": 1,
                "produto": "Livro de JavaScript",
                "quantidade": 2,
                "valor": 50.00
            },
            {
                "idPedido": 2,
                "produto": "Teclado Mecânico",
                "quantidade": 1,
                "valor": 100.00
            },
            {
                "idPedido": 3,
                "produto": "Mouse Sem Fio",
                "quantidade": 1,
                "valor": 40.00
            }
        ],
        "aniversario": new Date("1991-06-15T00:00:00Z"),
        "campoNulo": null,
        "fotoPerfil": new BinData(0, "binaryDataHere4"),
        "website": "https://www.lucas.com",
        "funcaoJS": function() { print("Hola!"); },
        "tag": "shopper"
    }
];

db.users.insertMany(users);
