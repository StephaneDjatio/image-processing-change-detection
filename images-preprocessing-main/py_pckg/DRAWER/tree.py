def draw_tree():
    # Définition des constantes
    TRUNK_WIDTH = 3
    TRUNK_HEIGHT = 5
    LEAF_HEIGHT = 8
    LEAF_WIDTH = 15

    # Calcul du nombre d'espaces pour centrer l'arbre
    center_spaces = ' ' * ((LEAF_WIDTH - 1) // 2)

    # Dessin de l'arbre
    for i in range(LEAF_HEIGHT):
        # Calcul du nombre d'espaces avant les feuilles
        leaf_spaces = ' ' * (LEAF_HEIGHT - i - 1)

        # Calcul du nombre de feuilles à afficher
        leaf_width = i * 2 + 1

        # Affichage des feuilles
        leaves = '*' * leaf_width
        print(f'{center_spaces}{leaf_spaces}{leaves}'.ljust(200 + LEAF_WIDTH))

    # Dessin du tronc
    for i in range(TRUNK_HEIGHT):
        # Calcul du nombre d'espaces avant le tronc
        trunk_spaces = ' ' * ((LEAF_WIDTH - TRUNK_WIDTH) // 2)

        # Affichage du tronc
        trunk = '|' * TRUNK_WIDTH
        print(f'{center_spaces}{trunk_spaces}{trunk}'.ljust(200 + LEAF_WIDTH))

    # Ajout des espaces pour centrer l'arbre
    centering_spaces = ' ' * ((LEAF_WIDTH - 1) // 2)
    print(centering_spaces.ljust(200 + LEAF_WIDTH))