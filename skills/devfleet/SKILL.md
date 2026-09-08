---
name: devfleet
description: "Instala, diagnostica y opera entornos locales DevFleet para worktrees Git de macOS. Úsala al preparar, aislar, observar o limpiar entornos React Native, iOS, Metro, procesos y Compose."
---

# DevFleet

Usa DevFleet para operar entornos locales vinculados a worktrees Git existentes.
No convierte repositorios ni modifica su configuración rastreada automáticamente.

## Descubrimiento

1. Confirma el worktree con `git rev-parse --show-toplevel`.
2. Usa el socket que entregue el usuario; si no hay uno, prueba
   `/tmp/df-$(id -u)/devfleet.sock`.
3. Comprueba daemon y plugins antes de actuar:

```sh
devfleet --socket "$socket" plugin list
devfleet --socket "$socket" doctor --worktree "$worktree"
```

Si `devfleet` no está instalado, pide el archivo de distribución o su ruta.
En el paquete macOS Apple Silicon, ejecuta `install-macos-arm64.sh`. El
instalador verifica checksums, conserva una CLI Docker ya existente (por ejemplo
OrbStack) y solo instala Docker Desktop con Homebrew si `docker` no existe.

## Confianza

El instalador de distribución puede confiar los cinco plugins oficiales después
de verificar el paquete. Para una instalación manual, inspecciona descriptor y
checksum antes de confiar un plugin oficial.

Nunca ejecutes `devfleet plugin trust <id>` para un plugin externo sin la
autorización explícita del usuario. La confirmación de una TUI futura debe
mostrar ID, checksum, descriptor y capacidades solicitadas; mientras no exista,
solicita esa confirmación claramente antes de usar el comando CLI.

`init --trust` registra una decisión de confianza del proyecto en estado privado.
Ejecuta esa opción solo cuando el usuario haya pedido preparar ese worktree.

## Operación

Empieza con `doctor` y `plan` cuando el usuario pide inspección o diagnóstico.
Para crear un entorno, usa `up --detach` con `--worktree` y un nombre de
instancia legible; conserva y muestra el `environment_id` y el `operation_id`
resultantes. Sigue operaciones con `operation`, `status`, `events` y `logs`.

Para Compose, verifica primero que haya un runtime Docker local funcional:

```sh
docker info
```

No instales Docker Desktop si OrbStack u otro runtime ya proporciona `docker`.
Para endpoints que necesitan un puerto de host automático, declara puerto `0`;
DevFleet inyecta el valor resuelto como `DEVFLEET_<ENDPOINT>_PORT`. Mantén el
binding en loopback y no expongas servicios sin una solicitud explícita.

Para iOS, ejecuta `doctor` antes de iniciar. No supongas que Homebrew instaló
Xcode o un runtime de Simulator: verifica Xcode, simuladores, Node y CocoaPods
en la máquina. Usa selecciones de runtime y tipo de dispositivo ya instalados.

## Cierre y evidencia

`stop` conserva estado, historial y artefactos para inspección. Usa `destroy`
solo con autorización explícita del usuario: elimina recursos verificados y
propiedad de DevFleet. Reporta IDs de entorno/operación, resultado de `doctor`,
estado del runtime Docker y cualquier bloqueo de Xcode, CocoaPods o Simulator.
