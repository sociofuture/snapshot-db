# snapshot-db

MySQL プロトコルを話す、**読み取り専用のスナップショット用サーバー**。

別の MySQL から「ある基準日の行」だけを抜き出して流し込み、しばらくの間 read-only の DB として公開するために作った。
書き込める手段は `LOAD DATA LOCAL INFILE` だけで、**一度作ったテーブルは二度と変更されない**。

```
元の MySQL ──(基準日の行を抽出)──▶ LOAD DATA LOCAL INFILE ──▶ snapshot-db ──▶ mysql CLI / BI ツールから SELECT
                                                                   │
                                                                   └─ SNAPSHOT SAVE ──▶ S3 ──(--restore)──▶ 再起動後に復元
```

基準日の違いはクエリではなくコンテナ(プロセス)で区別する想定。クエリに日付条件は要らない。

## 使い方

```bash
cargo run --release -- --listen 127.0.0.1:3307 --memory 512M
```

| オプション | 既定値 | 説明 |
|---|---|---|
| `--listen` | `127.0.0.1:3307` | 待ち受けアドレス |
| `--memory` | `256M` | メモリ領域(アリーナ)の大きさ。起動時に固定。`K` `M` `G` が使える |
| `--restore <URL>` | なし | 保存済みの画像から復元して起動する(`s3://bucket/prefix` または `file:///path`) |
| `--query-memory` | `512M` | 実行中のクエリ(JOIN のハッシュテーブル、ソート、集計)が使えるメモリの合計。`--memory` とは別枠 |
| `--max-rows` | `1000000` | 1つのクエリが返せる最大行数。`0` で無制限 |

**メモリの目安**: プロセス全体では `--memory` + `--query-memory` + 結果1バッチ分 + 少しのオーバーヘッドを使う。
コンテナのメモリ上限は、これより大きくしておく。

接続は普通の MySQL クライアントで行う。認証は現在チェックしていない(誰でも入れる)ため、公開するネットワークに注意。

```bash
mysql --host 127.0.0.1 --port 3307 --local-infile=1
```

### 1. テーブルを宣言する

```sql
SNAPSHOT CREATE TABLE employee (id BIGINT, name VARCHAR(50), pay DECIMAL(10,2), hired DATE)
  ROWS 10000 BYTES_PER_ROW_HINT 32;
```

- **`ROWS n` は必須**。メモリを先に確保するため、行数を宣言する。
- `BYTES_PER_ROW_HINT m`(既定 32): VARCHAR 列の1行あたりの平均バイト数。確保量は `ROWS × m`。
- 列定義の `NOT NULL`、`DEFAULT` などは読み飛ばす(`SHOW CREATE TABLE` の列定義を貼りやすい)。
- テーブル名と列名は**小文字にそろえて**保存される。

### 2. データを流し込む

```sql
LOAD DATA LOCAL INFILE 'employee.csv' INTO TABLE employee
  FIELDS TERMINATED BY ',' ENCLOSED BY '"' IGNORE 1 LINES;
```

- 区切りは `TERMINATED BY`(既定はタブ)、囲みは `ENCLOSED BY`、ヘッダー行は `IGNORE n LINES`。`\N` は NULL。
- **ファイルの行数が宣言の `ROWS` と違うとエラー**。型や日付が不正な行も、行番号と列を示してエラーになる。
  失敗したテーブルは宣言した直後の状態に戻り、やり直せる。
- **1つのテーブルをロードできるのは1回だけ**。成功したテーブルへの再ロードは拒否される。
  `REPLACE` / `IGNORE`(`LOAD DATA ... REPLACE INTO`)も使えない。

#### バイナリ(Arrow IPC)で流し込む: `FORMAT ARROW`

CSV の代わりに、[Arrow IPC ストリーム](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format)をそのまま送れる。
CSV のパースがなく、列のバッファをアリーナにコピーするだけなので速い。

```sql
LOAD DATA LOCAL INFILE 'employee.arrows' INTO TABLE employee FORMAT ARROW;
```

- ファイル名は形だけ(JDBC で `InputStream` を渡すときは使われない)。`FORMAT ARROW` は、このサーバー独自の書き方。
- **スキーマは宣言したテーブルと完全に一致させる**。列の数、名前(大文字小文字は無視)、Arrow の型が違うとエラー。型の変換はしない。

  | テーブルの型 | Arrow の型 |
  |---|---|
  | `TINYINT` `SMALLINT` `INT` `BIGINT` | `Int8` `Int16` `Int32` `Int64` |
  | `DOUBLE` | `Float64` |
  | `DECIMAL(p,s)` | `Decimal128(p, s)` |
  | `DATE` | `Date32` |
  | `DATETIME` `TIMESTAMP` | `Timestamp(Microsecond, タイムゾーンなし)` |
  | `VARCHAR` など | `Utf8`(`LargeUtf8` や `Utf8View`、辞書型は不可) |

- バッチは複数に分けてよい。ストリームの終端が来ていない(途切れた)場合もエラー。
- `ROWS` の照合、1回だけロード、失敗時のやり直しは CSV と同じ。

**Java から、メモリ上で作って渡す**例(MySQL Connector/J と Arrow Java)。

```java
// 接続URL: jdbc:mysql://host:3307/?allowLoadLocalInfile=true
ByteArrayOutputStream buf = new ByteArrayOutputStream();
try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(buf))) {
    w.start(); w.writeBatch(); w.end();   // root: VectorSchemaRoot(テーブルと同じスキーマ)
}

try (Statement st = conn.createStatement()) {
    st.unwrap(com.mysql.cj.jdbc.JdbcStatement.class)
      .setLocalInfileInputStream(new ByteArrayInputStream(buf.toByteArray()));
    st.execute("LOAD DATA LOCAL INFILE 'ignored.arrows' INTO TABLE employee FORMAT ARROW");
}
```

件数が多いときは、`PipedInputStream` などで、作りながら流すこともできる。
(この Java の例は、Connector/J の API の記憶に基づく。このリポジトリのテストは Rust の MySQL クライアントで行っており、Java では未確認。)

### 3. 読む

```sql
SELECT e.name, d.dept, s.amount
FROM employee e
JOIN dept d   ON d.emp_id = e.id
JOIN salary s ON s.emp_id = e.id
ORDER BY e.id;
```

- SQL の実行は [Apache DataFusion](https://datafusion.apache.org/)(MySQL 方言)。JOIN、集計、サブクエリ、ウィンドウ関数など。
- **書き込みになる文は拒否**: `INSERT` / `UPDATE` / `DELETE` / `DROP` / `CREATE TABLE AS` / `WITH ... INSERT` など。
- 日時は MySQL と同じ `YYYY-MM-DD HH:MM:SS` の形式で返す。

### 4. 状態を見る

```sql
SNAPSHOT STATUS;   -- テーブルごとの状態(Created / Loading / Loaded)、行数、確保バイト数、アリーナの使用量
```

## 対応している型

| 型 | 備考 |
|---|---|
| `TINYINT` `SMALLINT` `INT` `BIGINT` | `UNSIGNED` は書けるが符号付きの範囲で扱う |
| `DOUBLE` `FLOAT` | どちらも 8 バイトの `DOUBLE` として保存 |
| `DECIMAL(p,s)` | p は 38 まで。**丸めない**(桁があふれる値はエラー。末尾が 0 だけなら可) |
| `DATE` | `YYYY-MM-DD`。存在しない日付はエラー |
| `DATETIME` `TIMESTAMP` | `YYYY-MM-DD[ T]HH:MM:SS[.ffffff]`。タイムゾーンは扱わず、書かれた値のまま |
| `VARCHAR` `CHAR` `TEXT` | UTF-8。ロード時に検証 |

`ENUM` `JSON` `BLOB` などは未対応(宣言時にエラー)。

## 保存と復元(S3)

```sql
SNAPSHOT SAVE 's3://my-bucket/asof-2026-04-01';
```

```bash
snapshot-db --memory 512M --restore s3://my-bucket/asof-2026-04-01
```

- 保存先の下に `arena.bin`(アリーナの使用済み部分)と `manifest.json`(テーブルと列の配置、サイズ、SHA-256)ができる。
  **manifest は最後に書く**ので、途中で失敗した保存が完成品に見えることはない。
- アップロードは 8MB ずつ。サーバーは動いたまま保存できる(ロード済みのテーブルは変わらないため)。
  ロード中のテーブルがあるときは保存を拒否する。
- 復元は `arena.bin` を読み込み、SHA-256 を照合する。**壊れていると起動しない**。
  ロード済みのテーブルは復元後も不変。宣言だけでロードしていないテーブルは空で復元され、1回だけロードできる。
- `--memory` が画像より小さいときは、必要なサイズを示して起動に失敗する。
- 終了時の自動保存はない。`SNAPSHOT SAVE` を明示的に実行する。
- `file:///...` ならローカルのフォルダに保存できる。

### S3 の認証と設定

認証情報は、アクセスキーを渡さなければ次の順で自動的に探される(`object_store` の標準の動作)。

1. EKS の Pod(`AWS_WEB_IDENTITY_TOKEN_FILE`)
2. ECS のタスクロール(`AWS_CONTAINER_CREDENTIALS_*`)
3. **EC2 のインスタンスメタデータ(IMDSv2)**。インスタンスに付いた **IAM ロールがそのまま使える**。

そのため EC2 上では、キーの設定なしで動く想定(実際の EC2 での動作は未確認。ソースの読み取りによる)。
明示したいときだけ `AWS_ACCESS_KEY_ID` `AWS_SECRET_ACCESS_KEY` を環境変数で渡す。

**リージョンは指定する**。`AWS_REGION` がないと `us-east-1` と見なされる。

```bash
AWS_REGION=ap-northeast-1 snapshot-db --memory 512M --restore s3://my-bucket/asof-2026-04-01
```

IAM ロールに必要な権限(保存先のバケットとプレフィックスに対して):

| 操作 | 権限 |
|---|---|
| 保存 | `s3:PutObject` `s3:AbortMultipartUpload` |
| 復元 | `s3:GetObject` |
| どちらも | `s3:ListBucket`(バケットに対して) |

MinIO などは `AWS_ENDPOINT` と、http の場合は `AWS_ALLOW_HTTP=true` を指定する。

## 暴走したクエリへの備え

結合条件の書き忘れ(`CROSS JOIN` と同じ状態)などで、1億行を返すクエリが投げられることがある。

- **作業メモリの上限(`--query-memory`)**: JOIN のハッシュテーブルや、ソート、集計が上限を超えると、そのクエリだけが
  `Resources exhausted` で失敗する。サーバーと接続は生きている。**アリーナは JOIN では消費されない**(アリーナにはテーブルのデータだけが入る)。
- **結果の行数の上限(`--max-rows`)**: 結果はバッチごとに流して送る(全件をメモリに集めない)ので、結果が大きくてもメモリは一定。
  上限を超える前に止まり、`--max-rows` を示すエラーを返す。それまでに送った行は、クライアント側ではエラーとして扱われる。
- 上限を超えたクエリが失敗しても、メモリ上のデータは失われない。OOM でプロセスが落ちる事故を防ぐための仕組み。
- DataFusion の側に「すべての場合に上限が効くわけではない」という注意書きがある。上限に近い値で運用するときは、
  コンテナのメモリに余裕を持たせる。

## 設計のメモ

- **メモリは起動時に決める**。`--memory` の分を1回だけ確保し、足りなくなったらエラーにする。
  その場合は、サイズを大きくして再起動してやり直す(エラーに必要量が出る)。
- **データは連続した、ポインタを持たない領域**。テーブルの各列(有効ビット、値、文字列のオフセットとバイト列)は
  アリーナ内のオフセットで参照される。そのためファイルへの書き出しも読み込みも、バイト列をそのまま転送するだけで済む。
- DataFusion には、その領域を**コピーせずに**参照する Arrow のバッファとして渡す。
- 読み取りはロードが終わったテーブルだけ。ロード中のテーブルは見えない。

## ライブラリの改造(`vendor/opensrv-mysql`)

MySQL プロトコルは [opensrv-mysql](https://github.com/datafuselabs/opensrv) 0.7 を `vendor/` に取り込み、改造して使っている。

- `LOAD DATA LOCAL INFILE` の受信を追加(`local_infile_request` / `on_infile_data` / `on_infile_end`)。サーバーが `CLIENT_LOCAL_FILES` を名乗る。
- `PacketReader` の不具合を修正。次のパケットがすでにバッファにあるとき、返したパケットのメモリを解放してしまっていた
  (データと終端の空パケットが続けて届く LOAD DATA で内容が壊れた)。

## ビルドとテスト

```bash
cargo build --release
cargo test
```

テストは実際の MySQL クライアント(`mysql` クレート)でサーバーのプロセスに接続する。S3 への保存はローカルのフォルダで検証している
(本物の S3 での動作は未確認)。

## できないこと・今後

- **認証**: ユーザーとパスワードを検査していない(社内のネットワークでの利用が前提)。
- **SHOW / information_schema**: 未対応(BI ツールの接続時の問い合わせの一部は固定応答を返す)。
- **プリペアドステートメント**: 未対応(クライアント側で SQL を組み立てる接続は動く)。
- **インデックス**: なし。全件を読むが、列指向でメモリ上にあるため 1 万行規模では十分速い。必要になったら、
  `ALTER TABLE ... ADD INDEX` と独自の `TableProvider` で追加する想定。
- 数十万行以上でのテストはまだ行っていない。
