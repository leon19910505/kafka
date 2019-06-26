## Install

```
composer require zhizhong/kafka
```

## Config
set kafka brokers routes in your env ,it can be a list by string 

```
KAFKA_BROKERS=127.0.0.1:2086,127.0.0.2:2086
```


## Usage

#### Producer


the easy way to produce is use help function  **kafka_dispatch**

```
kafka_dispatch($topic, $msg)
```

> **Note:**  $msg  must be an array


#### Consumer
```php
$consumer = new Consumer($topic);
while (true) {
    $msg = $consumer->consume();
    print_r($msg);
}


