from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd

from compgraph import operations


def plot_img(data: operations.TRowsIterable, result_path: Path):
    df = pd.DataFrame(list(data))
    weekday_order = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    plt.figure(figsize=(12, 7))
    colors = plt.cm.Set3.colors # pyrefly: ignore
    for i, weekday in enumerate(weekday_order):
        day_data = df[df['weekday'] == weekday]
        if not day_data.empty:
            day_data = day_data.sort_values('hour') # pyrefly: ignore
            plt.plot(day_data['hour'], day_data['speed'],
                     marker='o',
                     linewidth=3,
                     markersize=8,
                     color=colors[i % len(colors)],
                     label=f'{weekday}',
                     alpha=0.8)

            for _, row in day_data.iterrows():
                plt.annotate(f"{row['speed']:.1f}",
                             (float(row['hour']), float(row['speed'])),
                             textcoords="offset points",
                             xytext=(0, 10),
                             ha='center',
                             fontsize=9,
                             fontweight='bold')

    plt.xlabel('Час дня', fontsize=12, fontweight='bold')
    plt.ylabel('Средняя скорость (км/ч)', fontsize=12, fontweight='bold')
    plt.title('Средняя скорость движения по часам и дням недели',
              fontsize=14, fontweight='bold')

    plt.grid(True, alpha=0.3, linestyle='--')
    plt.legend(title='День недели', fontsize=10, title_fontsize=11)

    plt.xticks(range(0, 25, 2), fontsize=10)
    plt.yticks(fontsize=10)
    plt.xlim(0, 24)
    plt.ylim(0, max(df['speed']) * 1.2)

    plt.gca().set_axisbelow(True)

    plt.tight_layout()
    plt.savefig(result_path, dpi=300, bbox_inches='tight')
